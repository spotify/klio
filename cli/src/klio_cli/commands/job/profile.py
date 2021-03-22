# Copyright 2020 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import pstats
import tempfile

import dateparser

from google.cloud import storage

from klio_cli.commands import base


class ProfilePipeline(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.job.profile"

    def __init__(
        self, job_dir, klio_config, docker_runtime_config, profile_config
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.profile_config = profile_config

    def _get_environment(self):
        envs = super()._get_environment()
        project = self.klio_config.pipeline_options.project
        if project:
            envs["GOOGLE_CLOUD_PROJECT"] = project
        return envs

    def _parse_timeit_flags(self, subcommand_flags):
        subcommand = ["timeit"]
        if subcommand_flags.get("iterations"):
            subcommand.extend(
                ["--iterations", str(subcommand_flags.get("iterations"))]
            )

        return subcommand

    def _parse_memory_per_line_flags(self, subcommand_flags):
        subcommand = ["memory-per-line"]
        if subcommand_flags.get("get_maximum"):
            subcommand.append("--maximum")
        else:
            # already defaults to true in klio-exec, just being safe/explicit
            subcommand.append("--per-element")
        return subcommand

    def _parse_memory_flags(self, subcommand_flags):
        subcommand = ["memory"]
        if subcommand_flags.get("interval"):
            subcommand.extend(
                ["--interval", str(subcommand_flags.get("interval"))]
            )
        if subcommand_flags.get("include_children"):
            subcommand.append("--include-children")
        if subcommand_flags.get("multiprocess"):
            subcommand.append("--multiprocess")
        if subcommand_flags.get("plot_graph"):
            subcommand.append("--plot-graph")

        return subcommand

    def _parse_cpu_flags(self, subcommand_flags):
        subcommand = ["cpu"]
        if subcommand_flags.get("interval"):
            subcommand.extend(
                ["--interval", str(subcommand_flags.get("interval"))]
            )
        if subcommand_flags.get("plot_graph"):
            subcommand.append("--plot-graph")

        return subcommand

    def _get_command(self, subcommand, subcommand_flags=None):
        command = ["profile"]

        cmd_to_parse_func = {
            "cpu": self._parse_cpu_flags,
            "memory": self._parse_memory_flags,
            "memory-per-line": self._parse_memory_per_line_flags,
            "timeit": self._parse_timeit_flags,
        }
        command.extend(cmd_to_parse_func[subcommand](subcommand_flags))

        if self.profile_config.input_file is not None:
            command.extend(["--input-file", self.profile_config.input_file])
        else:
            command.extend(self.profile_config.entity_ids)

        if self.profile_config.output_file:
            command.extend(["--output-file", self.profile_config.output_file])

        if self.profile_config.show_logs:
            command.append("--show-logs")

        return command

    def run(self, what, subcommand_flags):
        self._check_docker_setup()
        self._write_effective_config()
        self._setup_docker_image()

        kwargs = {"subcommand": what, "subcommand_flags": subcommand_flags}
        runflags = self._get_docker_runflags(**kwargs)
        self._run_docker_container(runflags)


class DataflowProfileStatsCollector(object):
    TMP_DIR_PREFIX = "klio-gcs-profile-data-"

    def __init__(
        self, gcs_location, since, until, input_file=None, output_file=None
    ):
        self.gcs_location = gcs_location
        self.since = since
        self.until = until
        self.output_file = output_file
        self.input_file = input_file
        if not input_file:
            self.since = DataflowProfileStatsCollector._parse_date(since)
            self.until = DataflowProfileStatsCollector._parse_date(until)

    @staticmethod
    def _parse_date(datestring):
        settings = {"TO_TIMEZONE": "UTC", "RETURN_AS_TIMEZONE_AWARE": True}
        return dateparser.parse(datestring, settings=settings)

    @staticmethod
    def _clean_restrictions(restrictions):
        # restrictions are passed in the CLI as strings, but they need to
        # be parsed before calling `pstats.Stats().print_stats`. Floats
        # are a % of top results, ints are # of top results, and strings
        # as a regex for filtering results; see docs:
        # docs.python.org/3/library/profile.html#pstats.Stats.print_stats
        for res in restrictions:
            if "." in res:
                try:
                    res = float(res)
                except Exception:
                    pass
            else:
                try:
                    res = int(res)
                except Exception:
                    pass
            yield res

    def _get_gcs_blobs(self):
        location = self.gcs_location.split("gs://")[1:][0]
        bucket_name, *folders = location.split("/")
        prefix = "/".join(folders)

        client = storage.Client()
        try:
            bucket = client.get_bucket(bucket_name)
        except Exception:
            logging.error(
                "Error getting bucket for profiling data", exc_info=True
            )
            raise SystemExit(1)

        try:
            # wrap all in try since `list_blobs` is a generator
            blobs = bucket.list_blobs(prefix=prefix)
            for blob in blobs:
                if blob.time_created < self.since:
                    continue
                if blob.time_created > self.until:
                    continue
                yield blob
        except Exception:
            logging.error(
                "Error getting profiling data from {}".format(bucket_name),
                exc_info=True,
            )
            raise SystemExit(1)

    def _download_gcs_file(self, blob, dest):
        name = blob.name.split("/")[-1]
        temp_output_file = os.path.join(dest, name)
        with open(temp_output_file, "wb") as dst:
            try:
                blob.download_to_file(dst)
                return temp_output_file
            except Exception:
                logging.error(
                    "Error downloading {}".format(name), exc_info=True
                )
                raise SystemExit(1)

    def _get_stats_object(self):
        if self.input_file:
            return pstats.Stats(self.input_file)

        blobs = self._get_gcs_blobs()
        tempdir = tempfile.TemporaryDirectory(
            dir=".", prefix=DataflowProfileStatsCollector.TMP_DIR_PREFIX
        )
        files_to_load = []
        for blob in blobs:
            files_to_load.append(self._download_gcs_file(blob, tempdir.name))

        return pstats.Stats(*files_to_load)

    def get(self, sort_stats, restrictions):
        restrictions = self._clean_restrictions(restrictions)

        stats = self._get_stats_object()

        if self.output_file:
            return stats.dump_stats(self.output_file)

        stats.sort_stats(*sort_stats)
        stats.print_stats(*restrictions)

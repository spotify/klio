# Copyright 2019-2020 Spotify AB
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

import atexit
import logging

import docker

from klio_cli import __version__ as klio_cli_version
from klio_cli.commands import base
from klio_cli.utils import docker_utils
from klio_cli.utils import stackdriver_utils as sd_utils


GCP_CRED_FILE = "gcloud/application_default_credentials.json"


class RunPipeline(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.job.run"

    def __init__(
        self, job_dir, klio_config, docker_runtime_config, run_job_config
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.run_job_config = run_job_config
        self.requires_config_file = True

    @staticmethod
    def _try_container_kill(container):
        try:
            container.kill()
        except docker.errors.APIError as e:
            logging.debug("Exception while killing container", exc_info=e)

    def _run_docker_container(self, runflags):
        container = self._docker_client.containers.run(**runflags)

        if self.run_job_config.direct_runner:
            atexit.register(self._try_container_kill, container)

        # TODO: container.logs(stream=True) redirects stderr to stdout.
        #       We should use appropriate streams so it's obvious to the use.
        #       (@jpvelez)
        for line in container.logs(stream=True):
            self._docker_logger.info(line.decode("utf-8").strip("\n"))

        exit_status = container.wait()["StatusCode"]

        if exit_status == 0 and not self.run_job_config.direct_runner:
            dashboard_name = sd_utils.DASHBOARD_NAME_TPL.format(
                job_name=self.klio_config.job_name,
                region=self.klio_config.pipeline_options.region,
            )
            base_err_msg = (
                "Could not find a Stackdriver dashboard for job '%s' that "
                "matched the name %s"
                % (self.klio_config.job_name, dashboard_name)
            )

            try:
                dashboard_url = sd_utils.get_stackdriver_group_url(
                    self.klio_config.pipeline_options.project,
                    self.klio_config.job_name,
                    self.klio_config.pipeline_options.region,
                )
            except Exception as e:
                logging.warning("%s: %s" % (base_err_msg, e))
            else:
                if dashboard_url:
                    logging.info(
                        "View the job's dashboard on Stackdriver: %s"
                        % dashboard_url
                    )
                else:
                    logging.warning(base_err_msg)

        return exit_status

    def _get_environment(self):
        envs = super()._get_environment()

        envs[
            "GOOGLE_CLOUD_PROJECT"
        ] = self.klio_config.pipeline_options.project
        envs["COMMIT_SHA"] = self.run_job_config.git_sha
        envs["KLIO_CLI_VERSION"] = klio_cli_version
        return envs

    def _get_command(self):
        command = ["run"]

        if self.docker_runtime_config.image_tag:
            command.extend(
                ["--image-tag", self.docker_runtime_config.image_tag]
            )

        if self.run_job_config.direct_runner:
            command.append("--direct-runner")

        if self.run_job_config.update is True:
            command.append("--update")
        elif (
            self.run_job_config.update is False
        ):  # don't do anything if `None`
            command.append("--no-update")

        return command

    def _setup_docker_image(self):
        super()._setup_docker_image()

        if not self.run_job_config.direct_runner:
            logging.info("Pushing worker image to GCR")
            docker_utils.push_image_to_gcr(
                self._full_image_name,
                self.docker_runtime_config.image_tag,
                self._docker_client,
            )

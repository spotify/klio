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
import subprocess

from klio_core import variables as var


def get_git_sha(cwd=None, image_tag=None):

    cmd = "git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty"
    try:
        return (
            subprocess.check_output(
                # pipe to devnull to suppress the error msgs from git itself
                cmd.split(),
                cwd=cwd,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError:
        if not image_tag:
            logging.error(
                "The directory from which you are running this is not a git "
                "directory, or has no commits yet. The latest commit is used "
                "to tag the Docker image that is built by this command. "
                "Consider overriding this value using the --image-tag flag "
                "until such a time as commits are available."
            )
            raise SystemExit(1)


# TODO: Move this to KlioConfig validation
#  once overriding & templates are done
def validate_dataflow_runner_config(klio_config):
    pipeline_opts = klio_config.pipeline_options.as_dict()
    mandatory_gcp_keys = [
        "project",
        "staging_location",
        "temp_location",
        "region",
    ]
    is_gcp = all(
        pipeline_opts.get(key) is not None for key in mandatory_gcp_keys
    )

    if not is_gcp:
        logging.error(
            "Unable to verify the mandatory configuration fields for"
            " DataflowRunner. Please fix job configuration or run via direct"
            "runner."
        )
        raise SystemExit(1)


def is_direct_runner(klio_config, direct_runner_flag):
    if direct_runner_flag:
        return True

    runner = klio_config.pipeline_options.runner
    if runner == var.KlioRunner.DIRECT_RUNNER:
        return True

    if runner == var.KlioRunner.DATAFLOW_RUNNER:
        validate_dataflow_runner_config(klio_config)

    return False


def import_gke_commands():
    # Importing GKE commands needs to be behind a try/except because the
    # kubernetes dependency is not part of the base install dependencies
    try:
        from klio_cli.commands.job import gke as gke_commands

        # the import is only local to this function so we need to return the
        # module
        return gke_commands
    except ImportError as e:
        if "kubernetes" in e.msg:
            logging.error(
                "Failed to import DirectGKERunner dependencies."
                " Did you install `klio-cli[kubernetes]`?"
            )
            raise SystemExit(1)
        logging.error(e)
        raise SystemExit(1)


def error_stackdriver_logger_metrics(klio_config, direct_runner):
    """Warn if deprecated + removed stackdriver_logger was configured."""
    if direct_runner:
        return
    if klio_config.pipeline_options.runner == var.KlioRunner.DIRECT_GKE_RUNNER:
        return

    metrics_config = klio_config.job_config.metrics
    if isinstance(metrics_config, dict):
        stackdriver_conf = metrics_config.get("stackdriver_logger")
        if stackdriver_conf not in (False, None):
            msg = (
                "The Stackdriver log-based metric client has been deprecated "
                "since 21.3.0 and removed in 21.10.0, in favor of the Native "
                "metrics client.\n"
                "See docs on how to turn on the native client for Dataflow: "
                "https://docs.klio.io/en/stable/userguide/pipeline/metrics.html"
                "#stackdriver-required-setup\n"
                "And docs how to configure the native client: "
                "https://docs.klio.io/en/stable/userguide/pipeline/metrics.html"
                "#configuration"
            )
            logging.error(msg)
            raise SystemExit(1)

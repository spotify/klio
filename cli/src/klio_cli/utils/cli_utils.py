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


def is_direct_runner(klio_config, direct_runner):
    if not direct_runner:
        validate_dataflow_runner_config(klio_config)

    return direct_runner

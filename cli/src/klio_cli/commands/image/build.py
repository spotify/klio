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

import os

import docker

from klio_cli.utils import docker_utils


def build(job_dir, conf, config_file, image_tag):
    image_name = conf.pipeline_options.worker_harness_container_image
    client = docker.from_env()

    if config_file:
        basename = os.path.basename(config_file)
        image_tag = "{}-{}".format(image_tag, basename)

    docker_utils.check_docker_connection(client)
    docker_utils.check_dockerfile_present(job_dir)
    docker_utils.build_docker_image(
        job_dir, image_name, image_tag, config_file
    )

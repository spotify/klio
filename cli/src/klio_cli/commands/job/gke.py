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

import logging

from klio_cli.commands import base
from klio_cli.utils import docker_utils


class RunPipelineGKE(base.BaseDockerizedPipeline):
    def __init__(self, job_dir, klio_config, docker_runtime_config):
        super().__init__(job_dir, klio_config, docker_runtime_config)

    def _update_deployment_yaml(self):
        # TODO : set the docker image in deployment.yaml
        pass

    def _apply_deployment(self):
        # TODO: kubectl apply
        pass

    def _setup_docker_image(self):
        super()._setup_docker_image()

        logging.info("Pushing worker image to GCR")
        docker_utils.push_image_to_gcr(
            self._full_image_name,
            self.docker_runtime_config.image_tag,
            self._docker_client,
        )

    def run(self, *args, **kwargs):
        # NOTE: Notice this job doesn't actually run docker locally, but we
        # still have to build and push the image before we can run kubectl

        # docker image setup
        self._check_gcp_credentials_exist()
        self._check_docker_setup()
        self._setup_docker_image()

        self._update_deployment_yaml()
        self._apply_deployment()

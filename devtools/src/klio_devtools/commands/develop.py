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

import os

import docker
import dockerpty

from klio_cli.commands import base


class DevelopKlioContainer(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.develop"
    PIP_CMD = "pip install -e /usr/src/{pkg}"
    # order is important - klio-core doesn't depend on any other
    # internal libraries; klio depends on klio-core; klio-exec depends
    # on klio-core & klio
    PKGS = ("klio-core", "klio", "klio-exec", "klio-audio")

    def __init__(
        self, job_dir, klio_config, docker_runtime_config, klio_path,
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.klio_path = klio_path

    def _run_docker_container(self, runflags):
        """
        Create & run job container, install klio packages as editable,
        and attach to the container with an interactive terminal.
        """
        container = self._docker_client.containers.create(**runflags)
        container.start()
        for pkg in self.PKGS:
            _, output = container.exec_run(
                self.PIP_CMD.format(pkg=pkg), tty=True, stream=True
            )
            for line in output:
                try:
                    self._docker_logger.info(line.decode("utf-8").strip("\n"))
                except Exception:
                    # sometimes there's a decode error for a log line, but it
                    # shouldn't stop the setup
                    pass

        # need to use lower-level Docker API client in order to start
        # an interactive terminal inside the running container
        self._docker_logger.info(
            "\nConnecting to job's container. Use CTRL+C to stop."
        )
        pty_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        dockerpty.start(pty_client, container.attrs)

    def _get_volumes(self):
        volumes = super(DevelopKlioContainer, self)._get_volumes()

        local_exec_path = os.path.join(self.klio_path, "exec")
        volumes[local_exec_path] = {"bind": "/usr/src/klio-exec", "mode": "rw"}

        local_core_path = os.path.join(self.klio_path, "core")
        volumes[local_core_path] = {"bind": "/usr/src/klio-core", "mode": "rw"}

        local_lib_path = os.path.join(self.klio_path, "lib")
        volumes[local_lib_path] = {"bind": "/usr/src/klio", "mode": "rw"}

        local_audio_path = os.path.join(self.klio_path, "audio")
        volumes[local_audio_path] = {
            "bind": "/usr/src/klio-audio",
            "mode": "rw",
        }

        return volumes

    def _get_docker_runflags(self, *args, **kwargs):
        return {
            "image": self._full_image_name,
            "entrypoint": "/bin/bash",
            "volumes": self._get_volumes(),
            "environment": self._get_environment(),
            "stdin_open": True,
            "tty": True,
        }

# Copyright 2021 Spotify AB
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
"""TODO: docstrings"""

import docker
import dockerpty

from klio_cli.commands import base


class InteractivePipeline(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.job.console"

    def __init__(
        self, job_dir, klio_config, docker_runtime_config, job_console_config
    ):
        super().__init__(job_dir, klio_config, docker_runtime_config)
        self.job_console_config = job_console_config

    def _run_docker_container(self, runflags):
        """
        Create & run job container, and start a Klio console.
        """
        container = self._docker_client.containers.create(**runflags)
        container.start()

        # need to use lower-level Docker API client in order to start
        # an interactive terminal inside the running container
        self._docker_logger.info(
            "\nConnecting to the Klio console in the job's container..."
        )
        pty_client = docker.APIClient(base_url="unix://var/run/docker.sock")
        dockerpty.start(pty_client, container.attrs)
        self._docker_logger.info("Cleaning up the job's container...")
        try:
            container.remove()
            self._docker_logger.debug(f"Successfully removed container {container.short_id}.")
        except Exception:
            self._docker_logger.warning(
                "Could not remove job's container - it may still be running. "
                f"Try running `docker kill {container.short_id}`."
            )

    def _get_command(self):
        command = [
            "console", 
            "--klio-cli-version", 
            self.job_console_config.klio_cli_version,
            "--active-config-file",
            self.job_console_config.config_file,
            "--image-name",
            self._full_image_name,
        ]
        docker_info = self._docker_client.info()
        docker_version = docker_info.get("ServerVersion")
        if docker_version:
            command.extend(["--docker-version", str(docker_version)])

        if self.job_console_config.notebook:
            command.append("--notebook")

        if self.job_console_config.ipython:
            command.append("--ipython")
        print(f"Running the following command: {command}")
        return command

    def _get_environment(self):
        envs = super(InteractivePipeline, self)._get_environment()
        envs["IMAGE_TAG"] = self.docker_runtime_config.image_tag
        envs["IMAGE_NAME"] = self._full_image_name
        docker_info = self._docker_client.info()
        docker_version = docker_info.get("ServerVersion")
        if docker_version:
            envs["DOCKER_VERSION"] = str(docker_version)
        envs["KLIO_CLI_VERSION"] = self.job_console_config.klio_cli_version
        envs["ACTIVE_CONFIG_FILE"] = self.job_console_config.config_file
        return envs

    def _get_docker_runflags(self, *args, **kwargs):
        return {
            "image": self._full_image_name,
            "entrypoint": self.ENTRYPOINT,
            "command": self._add_base_args(self._get_command(*args, **kwargs)),
            "volumes": self._get_volumes(),
            "environment": self._get_environment(),
            "stdin_open": True,
            "tty": True,
            # TODO: make port configurable
            # TODO: add port only if running a notebook
            "ports": {"8888/tcp": ("0.0.0.0", 8888)},
        }

# Copyright 2020 Spotify AB

from klio_cli.commands import base


class AuditPipeline(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.job.audit"

    def _get_environment(self):
        envs = super()._get_environment()
        envs["KLIO_TEST_MODE"] = "true"
        return envs

    def _get_command(self, list_steps):
        command = ["audit"]
        if list_steps:
            command.append("--list")
        return command

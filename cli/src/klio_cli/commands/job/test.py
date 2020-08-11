# Copyright 2019 Spotify AB

from klio_cli.commands import base


class TestPipeline(base.BaseDockerizedPipeline):
    DOCKER_LOGGER_NAME = "klio.job.test"

    def _get_environment(self):
        envs = super()._get_environment()
        envs["KLIO_TEST_MODE"] = "true"
        return envs

    def _get_command(self, pytest_args):
        return ["test"] + pytest_args

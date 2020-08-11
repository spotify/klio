# Copyright 2019 Spotify AB

import pytest

from klio_cli.commands.job import test as test_job


@pytest.fixture
def mock_os_environ(mocker):
    patch = {"HOME": "/home", "USER": "cookiemonster"}
    return mocker.patch.dict("os.environ", patch)


@pytest.fixture
def test_pipeline(mock_os_environ):
    return test_job.TestPipeline("job/dir", "klio_config", "docker_config")


def test_get_environment(test_pipeline):
    gcreds = "/usr/gcloud/application_default_credentials.json"
    exp_envs = {
        "PYTHONPATH": "/usr/src/app",
        "GOOGLE_APPLICATION_CREDENTIALS": gcreds,
        "USER": "cookiemonster",
        "KLIO_TEST_MODE": "true",
    }

    assert exp_envs == test_pipeline._get_environment()


def test_get_command(test_pipeline):
    assert ["test", "py", "args"] == test_pipeline._get_command(["py", "args"])

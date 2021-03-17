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
    assert ["test"] == test_pipeline._get_command()


def test_add_pytest_args(test_pipeline):
    docker_runflags = {"command": ["cmd"]}
    test_pipeline._add_pytest_args(docker_runflags["command"], ["py", "args"])
    assert ["cmd", "py", "args"] == docker_runflags["command"]


def test_get_docker_runflags(mocker, monkeypatch, test_pipeline):
    mock_base_docker_runflags = mocker.Mock()
    mock_base_docker_runflags.return_value = {
        "command": ["test", "--config-file", "x"]
    }
    monkeypatch.setattr(
        test_job.base.BaseDockerizedPipeline,
        "_get_docker_runflags",
        mock_base_docker_runflags,
    )
    actual_runflags = test_pipeline._get_docker_runflags(
        pytest_args=["py", "args"]
    )

    expected = ["test", "--config-file", "x", "py", "args"]
    assert expected == actual_runflags["command"]


def test_requires_config_setting(test_pipeline):
    assert test_pipeline.requires_config_file

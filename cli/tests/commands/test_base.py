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

import pytest

from klio_core import config

from klio_cli import cli
from klio_cli.commands import base


@pytest.fixture
def mock_os_environ(mocker):
    patch = {"HOME": "/home", "USER": "cookiemonster"}
    return mocker.patch.dict(base.os.environ, patch)


@pytest.fixture
def klio_config():
    config_dict = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {"worker_harness_container_image": "test-image"},
        "job_config": {
            "inputs": [
                {
                    "topic": "foo-topic",
                    "subscription": "foo-sub",
                    "data_location": "foo-input-location",
                }
            ],
            "outputs": [
                {
                    "topic": "foo-topic-output",
                    "data_location": "foo-output-locaiton",
                }
            ],
        },
    }
    return config.KlioConfig(config_dict)


@pytest.fixture
def docker_runtime_config():
    return cli.DockerRuntimeConfig(
        image_tag="foo-123", force_build=False, config_file_override=None,
    )


@pytest.fixture
def mock_docker_client(mocker):
    mock_client = mocker.Mock()
    mock_container = mocker.Mock()
    mock_container.logs.return_value = [b"a log line\n", b"another log line\n"]
    mock_container.wait.return_value = {"StatusCode": 0}
    mock_client.containers.run.return_value = mock_container
    return mock_client


@pytest.fixture
def mock_materialized_config_file(mocker):
    mock = mocker.Mock()
    mock.name = "test-config"
    return mock


@pytest.fixture
def base_pipeline(
    klio_config,
    docker_runtime_config,
    mock_docker_client,
    mock_os_environ,
    monkeypatch,
):
    job_dir = "/test/dir/jobs/test_run_job"
    pipeline = base.BaseDockerizedPipeline(
        job_dir=job_dir,
        klio_config=klio_config,
        docker_runtime_config=docker_runtime_config,
    )

    monkeypatch.setattr(pipeline, "_docker_client", mock_docker_client)
    return pipeline


@pytest.fixture
def expected_envs():
    gcreds = "/usr/gcloud/application_default_credentials.json"
    return {
        "PYTHONPATH": "/usr/src/app",
        "GOOGLE_APPLICATION_CREDENTIALS": gcreds,
        "USER": "cookiemonster",
    }


@pytest.fixture
def expected_volumes():
    return {
        "/home/.config/gcloud/application_default_credentials.json": {
            "bind": "/usr/gcloud/application_default_credentials.json",
            "mode": "rw",
        },
        "/test/dir/jobs/test_run_job": {"bind": "/usr/src/app", "mode": "rw"},
        "test-config": {
            "bind": base.BaseDockerizedPipeline.MATERIALIZED_CONFIG_PATH,
            "mode": "rw",
        },
    }


def test_get_docker_logger(base_pipeline):
    exp_logger = logging.getLogger(
        base.BaseDockerizedPipeline.DOCKER_LOGGER_NAME
    )

    assert exp_logger == base_pipeline._get_docker_logger()


def test_run_docker_container(base_pipeline, caplog):
    runflags = {"a": "flag"}

    base_pipeline._run_docker_container(runflags)

    base_pipeline._docker_client.containers.run.assert_called_once_with(
        **runflags
    )
    ret_container = base_pipeline._docker_client.containers.run.return_value
    ret_container.logs.assert_called_once_with(stream=True)
    ret_container.wait.assert_called_once_with()


def test_get_environment(base_pipeline, expected_envs):
    assert expected_envs == base_pipeline._get_environment()


def test_get_volumes(
    base_pipeline,
    expected_volumes,
    mocker,
    monkeypatch,
    mock_materialized_config_file,
):
    monkeypatch.setattr(
        base_pipeline,
        "materialized_config_file",
        mock_materialized_config_file,
    )
    assert expected_volumes == base_pipeline._get_volumes()


def test_get_command(base_pipeline):
    with pytest.raises(NotImplementedError):
        base_pipeline._get_command()


@pytest.mark.parametrize("requires_config", (False, True))
def test_get_docker_runflags(
    base_pipeline,
    expected_volumes,
    expected_envs,
    mocker,
    monkeypatch,
    requires_config,
    mock_materialized_config_file,
):
    mock_get_command = mocker.Mock(return_value=["command"])
    monkeypatch.setattr(base_pipeline, "_get_command", mock_get_command)

    base_pipeline.requires_config_file = requires_config

    expected_command = ["command"]

    if requires_config:
        monkeypatch.setattr(
            base_pipeline,
            "materialized_config_file",
            mock_materialized_config_file,
        )
        expected_command.extend(
            [
                "--config-file",
                base.BaseDockerizedPipeline.MATERIALIZED_CONFIG_PATH,
            ]
        )
    else:
        expected_volumes.pop("test-config")

    exp_runflags = {
        "image": "test-image:foo-123",
        "entrypoint": "klioexec",
        "command": expected_command,
        "volumes": expected_volumes,
        "environment": expected_envs,
        "detach": True,
        "auto_remove": True,
    }

    assert exp_runflags == base_pipeline._get_docker_runflags()


@pytest.mark.parametrize(
    "force_build,image_exists",
    ((False, False), (True, True), (False, True), (True, False),),
)
def test_setup_docker_image(
    base_pipeline, force_build, image_exists, mocker, monkeypatch,
):

    patch_rt_conf = base_pipeline.docker_runtime_config._replace(
        force_build=force_build
    )
    monkeypatch.setattr(base_pipeline, "docker_runtime_config", patch_rt_conf)

    mock_docker_image_exists = mocker.Mock(return_value=image_exists)
    monkeypatch.setattr(
        base.docker_utils, "docker_image_exists", mock_docker_image_exists
    )
    mock_build_docker_image = mocker.Mock()
    monkeypatch.setattr(
        base.docker_utils, "build_docker_image", mock_build_docker_image
    )

    base_pipeline._setup_docker_image()

    mock_docker_image_exists.assert_called_once_with(
        base_pipeline._full_image_name, base_pipeline._docker_client
    )

    if force_build or not image_exists:
        _pipe_opts = base_pipeline.klio_config.pipeline_options
        mock_build_docker_image.assert_called_once_with(
            base_pipeline.job_dir,
            _pipe_opts.worker_harness_container_image,
            base_pipeline.docker_runtime_config.image_tag,
            base_pipeline.docker_runtime_config.config_file_override,
        )
    else:
        mock_build_docker_image.assert_not_called()


def test_check_docker_setup(base_pipeline, mocker, monkeypatch):
    mock_check_docker_conn = mocker.Mock()
    monkeypatch.setattr(
        base.docker_utils, "check_docker_connection", mock_check_docker_conn
    )
    mock_check_dockerfile_present = mocker.Mock()
    monkeypatch.setattr(
        base.docker_utils,
        "check_dockerfile_present",
        mock_check_dockerfile_present,
    )

    base_pipeline._check_docker_setup()

    mock_check_docker_conn.assert_called_once_with(
        base_pipeline._docker_client
    )
    mock_check_dockerfile_present.assert_called_once_with(
        base_pipeline.job_dir
    )


def test_check_gcp_credentials_exist(base_pipeline, mocker, monkeypatch):
    mock_is_file = mocker.Mock(return_value=True)
    monkeypatch.setattr(base.os.path, "isfile", mock_is_file)

    base_pipeline._check_gcp_credentials_exist()

    exp_creds_path = (
        "/home/.config/gcloud/application_default_credentials.json"
    )
    mock_is_file.assert_called_once_with(exp_creds_path)


def test_check_gcp_credentials_exists_raises(
    base_pipeline, mocker, monkeypatch, caplog
):
    mock_is_file = mocker.Mock(return_value=False)
    monkeypatch.setattr(base.os.path, "isfile", mock_is_file)

    base_pipeline._check_gcp_credentials_exist()

    exp_creds_path = (
        "/home/.config/gcloud/application_default_credentials.json"
    )
    mock_is_file.assert_called_once_with(exp_creds_path)
    assert 1 == len(caplog.records)
    assert "WARNING" == caplog.records[0].levelname


def test_run(base_pipeline, mocker, monkeypatch):
    mock_check_gcp_credentials_exist = mocker.Mock()
    monkeypatch.setattr(
        base_pipeline,
        "_check_gcp_credentials_exist",
        mock_check_gcp_credentials_exist,
    )
    mock_check_docker_setup = mocker.Mock()
    monkeypatch.setattr(
        base_pipeline, "_check_docker_setup", mock_check_docker_setup
    )
    mock_setup_docker_image = mocker.Mock()
    monkeypatch.setattr(
        base_pipeline, "_setup_docker_image", mock_setup_docker_image
    )
    mock_get_docker_runflags = mocker.Mock()
    monkeypatch.setattr(
        base_pipeline, "_get_docker_runflags", mock_get_docker_runflags
    )
    mock_run_docker_container = mocker.Mock()
    monkeypatch.setattr(
        base_pipeline, "_run_docker_container", mock_run_docker_container
    )

    base_pipeline.run()

    mock_check_gcp_credentials_exist.assert_called_once_with()
    mock_check_docker_setup.assert_called_once_with()
    mock_setup_docker_image.assert_called_once_with()
    mock_get_docker_runflags.assert_called_once_with()
    mock_run_docker_container.assert_called_once_with(
        mock_get_docker_runflags.return_value
    )

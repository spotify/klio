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

from klio_core import config

from klio_cli import __version__ as klio_cli_version
from klio_cli import cli
from klio_cli.commands.job import run as run_job


@pytest.fixture
def mock_os_environ(mocker):
    return mocker.patch.dict(
        run_job.base.os.environ, {"USER": "cookiemonster"}
    )


@pytest.fixture
def klio_config():
    conf = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {
            "worker_harness_container_image": "test-image",
            "region": "some-region",
            "project": "test-project",
        },
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
                    "data_location": "foo-output-location",
                }
            ],
        },
    }
    return config.KlioConfig(conf)


@pytest.fixture
def docker_runtime_config():
    return cli.DockerRuntimeConfig(
        image_tag="foo-123",
        force_build=False,
        config_file_override="klio-job2.yaml",
    )


@pytest.fixture
def run_job_config():
    return cli.RunJobConfig(
        direct_runner=False, update=False, git_sha="12345678"
    )


@pytest.fixture
def mock_docker_client(mocker):
    mock_client = mocker.Mock()
    mock_container = mocker.Mock()
    mock_container.wait.return_value = {"StatusCode": 0}
    mock_container.logs.return_value = [b"a log line\n", b"another log line\n"]
    mock_client.containers.run.return_value = mock_container
    return mock_client


@pytest.fixture
def run_pipeline(
    klio_config,
    docker_runtime_config,
    run_job_config,
    mock_docker_client,
    mock_os_environ,
    monkeypatch,
):
    job_dir = "/test/dir/jobs/test_run_job"
    pipeline = run_job.RunPipeline(
        job_dir=job_dir,
        klio_config=klio_config,
        docker_runtime_config=docker_runtime_config,
        run_job_config=run_job_config,
    )

    monkeypatch.setattr(pipeline, "_docker_client", mock_docker_client)
    return pipeline


@pytest.mark.parametrize(
    "direct_runner,db_url",
    ((True, None), (False, "https://foo"), (False, None)),
)
def test_run_docker_container(
    direct_runner,
    db_url,
    run_pipeline,
    run_job_config,
    caplog,
    mocker,
    monkeypatch,
):
    run_job_config = run_job_config._replace(direct_runner=direct_runner)
    monkeypatch.setattr(run_pipeline, "run_job_config", run_job_config)

    mock_sd_utils = mocker.Mock()
    mock_sd_utils.get_stackdriver_group_url.return_value = db_url
    monkeypatch.setattr(run_job, "sd_utils", mock_sd_utils)

    runflags = {"a": "flag"}
    run_pipeline._run_docker_container(runflags)

    run_pipeline._docker_client.containers.run.assert_called_once_with(
        **runflags
    )
    ret_container = run_pipeline._docker_client.containers.run.return_value
    ret_container.logs.assert_called_once_with(stream=True)

    if not direct_runner:
        mock_sd_utils.get_stackdriver_group_url.assert_called_once_with(
            "test-project", "test-job", "some-region"
        )
        assert 1 == len(caplog.records)
    else:
        mock_sd_utils.get_stackdriver_group_url.assert_not_called()
        assert not len(caplog.records)


def test_failure_in_docker_container_returns_nonzero(
    run_pipeline, run_job_config, caplog, mocker, monkeypatch,
):
    mock_sd_utils = mocker.Mock()
    monkeypatch.setattr(run_job, "sd_utils", mock_sd_utils)

    container_run = run_pipeline._docker_client.containers.run
    container_run.return_value.wait.return_value = {"StatusCode": 1}
    runflags = {"a": "flag"}
    assert run_pipeline._run_docker_container(runflags) == 1

    container_run.assert_called_once_with(**runflags)
    ret_container = run_pipeline._docker_client.containers.run.return_value
    ret_container.logs.assert_called_once_with(stream=True)
    mock_sd_utils.get_stackdriver_group_url.assert_not_called()


def test_run_docker_container_dashboard_raises(
    run_pipeline, caplog, mocker, monkeypatch
):
    mock_sd_utils = mocker.Mock()
    mock_sd_utils.get_stackdriver_group_url.side_effect = Exception("fuu")
    monkeypatch.setattr(run_job, "sd_utils", mock_sd_utils)

    runflags = {"a": "flag"}
    run_pipeline._run_docker_container(runflags)

    run_pipeline._docker_client.containers.run.assert_called_once_with(
        **runflags
    )
    ret_container = run_pipeline._docker_client.containers.run.return_value
    ret_container.logs.assert_called_once_with(stream=True)

    mock_sd_utils.get_stackdriver_group_url.assert_called_once_with(
        "test-project", "test-job", "some-region"
    )
    assert 1 == len(caplog.records)


def test_get_environment(run_pipeline):
    gcreds = "/usr/gcloud/application_default_credentials.json"
    exp_envs = {
        "PYTHONPATH": "/usr/src/app",
        "GOOGLE_APPLICATION_CREDENTIALS": gcreds,
        "USER": "cookiemonster",
        "GOOGLE_CLOUD_PROJECT": "test-project",
        "COMMIT_SHA": "12345678",
        "KLIO_CLI_VERSION": klio_cli_version,
    }
    assert exp_envs == run_pipeline._get_environment()


@pytest.mark.parametrize(
    "config_file", (None, "klio-job2.yaml"),
)
@pytest.mark.parametrize(
    "image_tag,exp_image_flags",
    ((None, []), ("foo-123", ["--image-tag", "foo-123"])),
)
@pytest.mark.parametrize(
    "update,exp_update_flag",
    ((True, ["--update"]), (False, ["--no-update"]), (None, [])),
)
@pytest.mark.parametrize(
    "direct_runner,exp_runner_flag", ((False, []), (True, ["--direct-runner"]))
)
def test_get_command(
    direct_runner,
    exp_runner_flag,
    update,
    exp_update_flag,
    image_tag,
    exp_image_flags,
    config_file,
    run_pipeline,
    monkeypatch,
):
    run_job_config = run_pipeline.run_job_config._replace(
        direct_runner=direct_runner, update=update
    )
    monkeypatch.setattr(run_pipeline, "run_job_config", run_job_config)
    runtime_config = run_pipeline.docker_runtime_config._replace(
        image_tag=image_tag, config_file_override=config_file
    )
    monkeypatch.setattr(run_pipeline, "docker_runtime_config", runtime_config)

    exp_command = ["run"]
    exp_command.extend(exp_update_flag)
    exp_command.extend(exp_runner_flag)
    exp_command.extend(exp_image_flags)

    assert sorted(exp_command) == sorted(run_pipeline._get_command())


@pytest.mark.parametrize("direct_runner", (True, False))
def test_setup_docker_image(
    direct_runner, run_pipeline, mock_docker_client, mocker, monkeypatch
):
    run_job_config = run_pipeline.run_job_config._replace(
        direct_runner=direct_runner
    )
    monkeypatch.setattr(run_pipeline, "run_job_config", run_job_config)

    mock_super = mocker.Mock()
    monkeypatch.setattr(
        run_job.base.BaseDockerizedPipeline, "_setup_docker_image", mock_super
    )

    mock_docker_utils = mocker.Mock()
    monkeypatch.setattr(run_job, "docker_utils", mock_docker_utils)

    run_pipeline._setup_docker_image()

    mock_super.assert_called_once_with()
    if not direct_runner:
        mock_docker_utils.push_image_to_gcr.assert_called_once_with(
            "test-image:foo-123", "foo-123", mock_docker_client,
        )
    else:
        mock_docker_utils.push_image_to_gcr.assert_not_called()

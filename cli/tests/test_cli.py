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

import copy
import os

import click
import pytest

from click import testing

from klio_core import _testing as core_testing
from klio_core import config as kconfig
from klio_core import utils as core_utils

from klio_cli import cli
from klio_cli.commands.job import gke as gke_commands
from klio_cli.utils import cli_utils


@pytest.fixture
def mock_klio_config(mocker, monkeypatch, patch_os_getcwd):
    return core_testing.MockKlioConfig(
        cli, mocker, monkeypatch, patch_os_getcwd
    )


@pytest.fixture
def mock_create(mocker):
    return mocker.patch.object(cli.job_commands.create.CreateJob, "create")


@pytest.fixture
def mock_delete(mocker):
    return mocker.patch.object(
        cli.job_commands.delete, "DeleteJob", autospec=True
    )


@pytest.fixture
def mock_stop(mocker):
    stop_job = mocker.patch.object(cli.job_commands.stop, "StopJob")
    return stop_job.return_value.stop


@pytest.fixture
def runner():
    return testing.CliRunner()


@pytest.fixture
def config_file(patch_os_getcwd):
    conf = os.path.join(patch_os_getcwd, "klio-job.yaml")
    return conf


@pytest.fixture
def mock_warn_if_py2_job(mocker, monkeypatch):
    mock_warn = mocker.Mock()
    monkeypatch.setattr(cli.core_utils, "warn_if_py2_job", mock_warn)
    return mock_warn


@pytest.fixture
def mock_error_stackdriver_logger_metrics(mocker, monkeypatch):
    mock_error = mocker.Mock()
    monkeypatch.setattr(
        cli_utils, "error_stackdriver_logger_metrics", mock_error
    )
    return mock_error


@pytest.fixture
def mock_get_config_job_dir(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(cli.core_utils, "get_config_job_dir", mock)
    return mock


@pytest.fixture
def mock_get_git_sha(mocker, monkeypatch):
    mock_git_sha = mocker.Mock(return_value="12345678")
    monkeypatch.setattr(cli_utils, "get_git_sha", mock_git_sha)
    return mock_git_sha


@pytest.fixture
def minimal_config_data():
    return {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature"
        },
        "job_config": {
            "events": {
                "inputs": [
                    {
                        "type": "pubsub",
                        "topic": "foo-topic",
                        "subscription": "foo-sub",
                    }
                ],
                "outputs": [{"type": "pubsub", "topic": "foo-topic-output"}],
            },
            "data": {
                "inputs": [{"type": "gcs", "location": "foo-input-location"}],
                "outputs": [
                    {"type": "gcs", "location": "foo-output-location"}
                ],
            },
        },
    }


@pytest.fixture
def minimal_mock_klio_config(
    mock_klio_config, config_file, minimal_config_data
):

    mock_klio_config.setup(minimal_config_data, config_file)
    return mock_klio_config


@pytest.mark.parametrize(
    "image_tag,config_override",
    (
        (None, None),
        ("foobar", None),
        (None, "klio-job2.yaml"),
        ("foobar", "klio-job2.yaml"),
    ),
)
def test_build_image(
    image_tag,
    config_override,
    runner,
    mocker,
    config_file,
    mock_klio_config,
    mock_get_git_sha,
):
    mock_build = mocker.patch.object(cli.image_commands.build, "build")

    cli_inputs = ["image", "build"]
    if image_tag:
        cli_inputs.extend(["--image-tag", image_tag])
    if config_override:
        cli_inputs.extend(["--config-file", config_override])

    config = {
        "job_name": "test-job",
        "version": 1,
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

    conf_obj = mock_klio_config.setup(config, config_file, config_override)

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    if image_tag:
        mock_get_git_sha.assert_not_called()
    else:
        mock_get_git_sha.assert_called_once_with(
            mock_klio_config.patch_os_getcwd
        )

    mock_klio_config.assert_calls()

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    mock_build.assert_called_once_with(
        mock_klio_config.patch_os_getcwd,
        conf_obj,
        config_override,
        image_tag=exp_image_tag,
    )


def test_create_job(runner, mock_create, patch_os_getcwd):
    cli_inputs = [
        "job",
        "create",
        "--job-name",
        "test-job",
        "--gcp-project",
        "test-gcp-project",
        "--use-defaults",
    ]
    result = runner.invoke(cli.main, cli_inputs)
    core_testing.assert_execution_success(result)
    assert "" == result.output

    known_kwargs = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
        "use_defaults": True,
    }
    mock_create.assert_called_once_with((), known_kwargs, patch_os_getcwd)


def test_create_job_prompts(runner, mock_create, patch_os_getcwd):
    cli_inputs = ["job", "create"]
    prompt_inputs = ["test-job", "test-gcp-project"]
    inputs = "\n".join(prompt_inputs)
    result = runner.invoke(cli.main, cli_inputs, input=inputs)
    core_testing.assert_execution_success(result)

    exp_output = (
        "Name of your new job: {0}\n"
        "Name of the GCP project the job should be created in: "
        "{1}\n".format(*prompt_inputs)
    )
    assert exp_output == result.output

    known_kwargs = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
        "use_defaults": False,
    }
    mock_create.assert_called_once_with((), known_kwargs, patch_os_getcwd)


def test_create_job_unknown_args(runner, mock_create, patch_os_getcwd):
    cli_inputs = [
        "job",
        "create",
        "--job-name",
        "test-job",
        "--gcp-project",
        "test-gcp-project",
        "--use-defaults",
        "--input-topic",
        "test-input-topic",
    ]
    result = runner.invoke(cli.main, cli_inputs)
    core_testing.assert_execution_success(result)
    assert "" == result.output

    unknown_args = ("--input-topic", "test-input-topic")
    known_kwargs = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
        "use_defaults": True,
    }
    mock_create.assert_called_once_with(
        unknown_args, known_kwargs, patch_os_getcwd
    )


@pytest.mark.parametrize("config_override", (None, "klio-job2.yaml"))
def test_delete_job(
    config_override,
    mocker,
    monkeypatch,
    runner,
    config_file,
    mock_delete,
    patch_os_getcwd,
    mock_klio_config,
):

    config = {
        "job_name": "test-job",
        "version": 1,
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
    conf_obj = mock_klio_config.setup(config, config_file, config_override)

    cli_inputs = ["job", "delete"]
    if config_override:
        cli_inputs.extend(["--config-file", config_override])
    result = runner.invoke(cli.main, cli_inputs)
    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()
    mock_delete.assert_called_once_with(conf_obj)
    mock_delete.return_value.delete.assert_called_once_with()


def test_delete_job_gke(
    mocker, monkeypatch, runner, patch_os_getcwd, mock_klio_config,
):

    config = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {
            "project": "test-project",
            "runner": "DirectGKERunner",
        },
        "job_config": {},
    }
    mock_klio_config.setup(config, None, None)

    mock_delete_gke = mocker.patch.object(
        cli.job_commands.gke, "DeletePipelineGKE"
    )

    cli_inputs = ["job", "delete"]
    result = runner.invoke(cli.main, cli_inputs)
    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()
    mock_delete_gke.assert_called_once_with(mock_klio_config.meta.job_dir)
    mock_delete_gke.return_value.delete.assert_called_once_with()


@pytest.mark.parametrize(
    "is_gcp,direct_runner,config_override,image_tag",
    (
        (False, True, None, None),
        (True, False, None, None),
        (True, True, None, None),
        (True, True, None, "foobar"),
        (True, True, "klio-job2.yaml", None),
        (True, True, "klio-job2.yaml", "foobar"),
    ),
)
@pytest.mark.parametrize("is_job_dir_override", (False, True))
def test_run_job(
    runner,
    mocker,
    tmpdir,
    is_gcp,
    direct_runner,
    image_tag,
    config_override,
    config_file,
    mock_get_git_sha,
    mock_klio_config,
    mock_error_stackdriver_logger_metrics,
    is_job_dir_override,
):
    mock_run = mocker.patch.object(cli.job_commands.run.RunPipeline, "run")
    mock_run.return_value = 0

    config_data = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature",
            "project": "test-project",
            "region": "boonies",
            "staging_location": "gs://somewhere/over/the/rainbow",
            "temp_location": "gs://somewhere/over/the/rainbow",
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
    job_dir_override = None
    if is_job_dir_override:
        temp_dir = tmpdir.mkdir("testing12345")
        job_dir_override = str(temp_dir)

    mock_klio_config.setup(
        config_data, config_file, config_override, job_dir_override
    )

    cli_inputs = ["job", "run"]
    if image_tag:
        cli_inputs.extend(["--image-tag", image_tag])
    if direct_runner:
        cli_inputs.append("--direct-runner")
    if config_override:
        cli_inputs.extend(["--config-file", config_override])
    if job_dir_override:
        cli_inputs.extend(["--job-dir", job_dir_override])

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    if config_override:
        exp_image_tag = "{}-{}".format(exp_image_tag, config_override)

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()
    mock_run.assert_called_once_with()
    mock_get_git_sha.assert_called_once_with(
        mock_klio_config.meta.job_dir, image_tag
    )
    mock_error_stackdriver_logger_metrics.assert_called_once_with(
        mock_klio_config.klio_config, direct_runner
    )


@pytest.mark.parametrize(
    "direct_runner,config_override,image_tag",
    (
        (True, None, None),
        (False, None, None),
        (True, None, "foobar"),
        (True, "klio-job2.yaml", None),
        (True, "klio-job2.yaml", "foobar"),
    ),
)
def test_run_job_gke(
    runner,
    mocker,
    tmpdir,
    direct_runner,
    image_tag,
    config_override,
    config_file,
    mock_get_git_sha,
    mock_klio_config,
    mock_error_stackdriver_logger_metrics,
):
    mock_run_gke = mocker.patch.object(gke_commands.RunPipelineGKE, "run")
    mock_run_gke.return_value = 0
    mock_run = mocker.patch.object(cli.job_commands.run.RunPipeline, "run")
    mock_run.return_value = 0

    config_data = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature",
            "project": "test-project",
            "region": "boonies",
            "staging_location": "gs://somewhere/over/the/rainbow",
            "temp_location": "gs://somewhere/over/the/rainbow",
            "runner": "DirectGKERunner",
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

    mock_klio_config.setup(config_data, config_file, config_override)

    cli_inputs = ["job", "run"]
    if image_tag:
        cli_inputs.extend(["--image-tag", image_tag])
    if direct_runner:
        cli_inputs.append("--direct-runner")
    if config_override:
        cli_inputs.extend(["--config-file", config_override])

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    if config_override:
        exp_image_tag = "{}-{}".format(exp_image_tag, config_override)

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()

    if direct_runner:
        mock_run.assert_called_once_with()
        mock_run_gke.assert_not_called()
    else:
        mock_run_gke.assert_called_once_with()
        mock_run.assert_not_called()

    mock_get_git_sha.assert_called_once_with(
        mock_klio_config.meta.job_dir, image_tag
    )
    mock_error_stackdriver_logger_metrics.assert_called_once_with(
        mock_klio_config.klio_config, direct_runner
    )


def test_run_job_raises(
    runner,
    mocker,
    config_file,
    patch_os_getcwd,
    pipeline_config_dict,
    mock_warn_if_py2_job,
    mock_get_git_sha,
):
    mock_run = mocker.patch.object(cli.job_commands.run.RunPipeline, "run")
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")

    cli_inputs = ["job", "run", "--image-tag", "foobar"]

    config = {"job_name": "test-job"}

    mock_get_config.return_value = config

    result = runner.invoke(cli.main, cli_inputs)

    assert 1 == result.exit_code

    mock_warn_if_py2_job.asset_called_once_with(patch_os_getcwd)
    mock_get_config.assert_called_once_with(config_file)
    mock_get_git_sha.assert_not_called()
    mock_run.assert_not_called()


@pytest.mark.parametrize("config_override", (None, "klio-job2.yaml"))
def test_stop_job(
    config_override,
    runner,
    mock_stop,
    mocker,
    config_file,
    pipeline_config_dict,
    mock_klio_config,
):
    config_data = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": pipeline_config_dict,
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
    mock_klio_config.setup(config_data, config_file, config_override)

    cli_inputs = ["job", "stop"]
    if config_override:
        cli_inputs.extend(["--config-file", config_override])

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()
    mock_stop.assert_called_once_with(
        "test-job", "test-project", "us-central1", "cancel"
    )


def test_stop_job_gke(
    runner, mocker, mock_klio_config,
):
    config_data = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {
            "project": "test-project",
            "runner": "DirectGKERunner",
        },
        "job_config": {},
    }
    mock_klio_config.setup(config_data, None, None)

    mock_stop_gke = mocker.patch.object(
        cli.job_commands.gke, "StopPipelineGKE"
    )

    cli_inputs = ["job", "stop"]

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_klio_config.assert_calls()
    mock_stop_gke.assert_called_once_with(mock_klio_config.meta.job_dir)


@pytest.mark.parametrize(
    "is_gcp,direct_runner,config_override,image_tag",
    (
        (False, True, None, None),
        (True, False, None, None),
        (True, True, None, None),
        (True, True, "klio-job2.yaml", None),
    ),
)
def test_deploy_job(
    runner,
    mocker,
    is_gcp,
    direct_runner,
    config_override,
    image_tag,
    mock_stop,
    config_file,
    patch_os_getcwd,
    pipeline_config_dict,
    mock_get_git_sha,
    mock_klio_config,
    mock_error_stackdriver_logger_metrics,
):
    mock_run = mocker.patch.object(cli.job_commands.run.RunPipeline, "run")
    mock_run.return_value = 0

    config_data = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": pipeline_config_dict,
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

    mock_klio_config.setup(
        copy.deepcopy(config_data), config_file, config_override
    )

    cli_inputs = ["job", "deploy"]
    if image_tag:
        cli_inputs.extend(["--image_tag", image_tag])
    if direct_runner:
        cli_inputs.append("--direct-runner")
    if config_override:
        cli_inputs.extend(["--config-file", config_override])

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    if config_override:
        exp_image_tag = "{}-{}".format(exp_image_tag, config_override)

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_get_git_sha.assert_called_once_with(patch_os_getcwd, image_tag)
    mock_klio_config.assert_calls()
    mock_stop.assert_called_once_with(
        "test-job", "test-project", "us-central1", "cancel"
    )
    mock_run.assert_called_once_with()
    mock_error_stackdriver_logger_metrics.assert_called_once_with(
        mock_klio_config.klio_config, direct_runner
    )


def test_deploy_job_raises(
    runner,
    mocker,
    mock_stop,
    config_file,
    patch_os_getcwd,
    pipeline_config_dict,
    mock_warn_if_py2_job,
    mock_get_git_sha,
):
    mock_run = mocker.patch.object(cli.job_commands.run.RunPipeline, "run")
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")

    cli_inputs = ["job", "deploy", "--image-tag", "foobar"]

    config = {"job_name": "test-job"}

    mock_get_config.return_value = config

    result = runner.invoke(cli.main, cli_inputs)

    assert 1 == result.exit_code

    mock_warn_if_py2_job.assert_called_once_with(patch_os_getcwd)
    mock_get_config.assert_called_once_with(config_file)
    mock_stop.assert_not_called()
    mock_get_git_sha.assert_not_called()
    mock_run.assert_not_called()


@pytest.mark.parametrize(
    "pytest_args,conf_override,image_tag",
    (
        ([], None, None),
        (["--", "-s"], None, None),
        (["--", "test::test test2::test2"], None, None),
        (["--", "-s"], "klio-job2.yaml", None),
    ),
)
def test_test_job(
    runner,
    mocker,
    config_file,
    patch_os_getcwd,
    pytest_args,
    conf_override,
    image_tag,
    mock_get_git_sha,
    mock_warn_if_py2_job,
    mock_get_config_job_dir,
):
    mock_test_pipeline = mocker.patch.object(
        cli.job_commands.test, "TestPipeline"
    )
    mock_test_pipeline.return_value.run.return_value = 0
    mock_get_config_job_dir.return_value = (
        patch_os_getcwd,
        conf_override or config_file,
    )

    cli_inputs = ["job", "test"]
    if image_tag:
        cli_inputs.extend(["--image-tag", image_tag])
    if conf_override:
        cli_inputs.extend(["--config-file", conf_override])
    cli_inputs.extend(pytest_args)

    config_data = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature",
            "project": "test-project",
            "region": "boonies",
            "staging_location": "gs://somewhere/over/the/rainbow",
            "temp_location": "gs://somewhere/over/the/rainbow",
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
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")
    # deepcopy since KlioConfig will pop keys
    mock_get_config.return_value = config_data
    conf = kconfig.KlioConfig(copy.deepcopy(config_data))
    mock_klio_config = mocker.patch.object(core_utils.config, "KlioConfig")
    mock_klio_config.return_value = conf

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    if conf_override:
        exp_image_tag = "{}-{}".format(exp_image_tag, conf_override)
    mock_get_config_job_dir.assert_called_once_with(None, conf_override)
    mock_warn_if_py2_job.assert_called_once_with(patch_os_getcwd)
    if not image_tag:
        mock_get_git_sha.assert_called_once_with(patch_os_getcwd)
    else:
        mock_get_git_sha.assert_not_called()

    mock_get_config.assert_called_once_with(conf_override or config_file)
    mock_klio_config.assert_called_once_with(
        config_data, raw_overrides=(), raw_templates=()
    )
    exp_docker_runtime_config = cli.DockerRuntimeConfig(
        image_tag=exp_image_tag,
        force_build=False,
        config_file_override=conf_override,
    )
    mock_test_pipeline.assert_called_once_with(
        patch_os_getcwd, conf, exp_docker_runtime_config
    )
    mock_test_pipeline.return_value.run.assert_called_once_with(
        pytest_args=pytest_args
    )


@pytest.mark.parametrize(
    "create_resources,conf_override",
    (
        (False, None),
        (False, "klio-job2.yaml"),
        (True, None),
        (True, "klio-job2.yaml"),
    ),
)
def test_verify(
    runner,
    mocker,
    patch_os_getcwd,
    create_resources,
    conf_override,
    mock_warn_if_py2_job,
    mock_get_config_job_dir,
):
    mock_verify_job = mocker.patch.object(
        cli.job_commands.verify.VerifyJob, "verify_job"
    )
    mock_get_config_job_dir.return_value = (
        patch_os_getcwd,
        conf_override or config_file,
    )
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")

    config = {
        "job_name": "klio-job-name",
        "job_config": {
            "inputs": [
                {"topic": "foo", "subscription": "bar", "data_location": "baz"}
            ],
            "outputs": [{"topic": "foo-out", "data_location": "baz-out"}],
        },
        "pipeline_options": {},
        "version": 1,
    }
    mock_get_config.return_value = config

    cli_inputs = ["job", "verify"]
    if create_resources:
        cli_inputs.append("--create-resources")
    if conf_override:
        cli_inputs.extend(["--config-file", conf_override])

    result = runner.invoke(cli.main, cli_inputs)
    core_testing.assert_execution_success(result)

    mock_get_config_job_dir.assert_called_once_with(None, conf_override)
    mock_get_config.assert_called_once_with(conf_override or config_file)
    mock_warn_if_py2_job.assert_called_once_with(patch_os_getcwd)
    mock_verify_job.assert_called_once_with()


@pytest.mark.parametrize(
    "use_job_dir,gcs_location,conf_override",
    (
        (None, None, None),
        (True, None, None),
        (None, "gs://bar", None),
        (None, None, "klio-job2.yaml"),
    ),
)
@pytest.mark.parametrize(
    "input_file,output_file",
    (
        (None, None),
        ("input.stats", None),
        (None, "output.stats"),
        ("input.stats", "output.stats"),
    ),
)
@pytest.mark.parametrize(
    "since,until", ((None, None), ("2 hours ago", "1 hour ago"))
)
def test_collect_profiling_data(
    runner,
    patch_os_getcwd,
    mock_warn_if_py2_job,
    mocker,
    use_job_dir,
    gcs_location,
    conf_override,
    input_file,
    output_file,
    since,
    until,
):
    mock_collector = mocker.patch.object(
        cli.job_commands.profile, "DataflowProfileStatsCollector"
    )
    config = {
        "version": 2,
        "job_name": "test-job",
        "job_config": {"events": {}, "data": {}},
        "pipeline_options": {},
    }
    if not gcs_location:
        config["pipeline_options"] = {"profile_location": "gs://foo"}
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")
    mock_get_config.return_value = config

    exp_exit_code = 0
    exp_output_snippet = ""
    if input_file:
        if any([use_job_dir, gcs_location, output_file, conf_override]):
            exp_exit_code = 2
            exp_output_snippet = "mutually exclusive"
        elif any([since, until]):
            exp_output_snippet = "`--since` and `--until`  will be ignored"

    cli_inputs = ["job", "profile", "collect-profiling-data"]
    if input_file:
        cli_inputs.extend(["--input-file", input_file])
    if output_file:
        cli_inputs.extend(["--output-file", output_file])
    if since:
        cli_inputs.extend(["--since", since])
    if until:
        cli_inputs.extend(["--until", until])
    if gcs_location:
        cli_inputs.extend(["--gcs-location", gcs_location])
    if conf_override:
        cli_inputs.extend(["--config-file", conf_override])

    exp_since = since or "1 hour ago"
    exp_until = until or "now"

    with runner.isolated_filesystem() as f:
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")

        if use_job_dir:
            cli_inputs.extend(["--job-dir", f])

        result = runner.invoke(cli.main, cli_inputs)
        assert exp_exit_code == result.exit_code
        assert exp_output_snippet in result.output

        exp_job_dir = patch_os_getcwd if not use_job_dir else f
        if use_job_dir and exp_exit_code == 0:
            mock_warn_if_py2_job.assert_called_once_with(exp_job_dir)

        if exp_exit_code != 2:
            mock_collector.assert_called_once_with(
                gcs_location=gcs_location or "gs://foo",
                input_file=input_file,
                output_file=output_file,
                since=exp_since,
                until=exp_until,
            )
            mock_collector.return_value.get.assert_called_once_with(
                ("tottime",), ()
            )


def test_collect_profiling_data_raises(
    runner, patch_os_getcwd, mock_warn_if_py2_job, mocker
):
    mock_collector = mocker.patch.object(
        cli.job_commands.profile, "DataflowProfileStatsCollector"
    )
    config = {
        "version": 2,
        "job_name": "test-job",
        "job_config": {"events": {}, "data": {}},
        "pipeline_options": {},
    }
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")
    mock_get_config.return_value = config

    cli_inputs = ["job", "profile", "collect-profiling-data"]

    result = runner.invoke(cli.main, cli_inputs)

    # no need to check the whole error msg string
    assert "Error: Please provide a GCS location" in result.output
    assert 2 == result.exit_code

    mock_collector.assert_not_called()
    mock_collector.return_value.get.assert_not_called()


@pytest.mark.parametrize(
    "input_file,entity_ids,exp_raise,exp_msg",
    (
        ("input.txt", (), False, None),
        (None, ("foo", "bar"), False, None),
        ("input.txt", ("foo", "bar"), True, "Illegal usage"),
        (None, (), True, "Must provide"),
    ),
)
def test_require_profile_input_data(
    input_file, entity_ids, exp_raise, exp_msg
):
    if exp_raise:
        with pytest.raises(click.UsageError, match=exp_msg):
            cli._require_profile_input_data(input_file, entity_ids)
    else:
        ret = cli._require_profile_input_data(input_file, entity_ids)
        assert ret is None


@pytest.mark.parametrize(
    "image_tag,config_override", ((None, None), ("foo-1234", "klio-job2.yaml"))
)
def test_profile(
    image_tag,
    config_override,
    config_file,
    patch_os_getcwd,
    pipeline_config_dict,
    mock_get_git_sha,
    mocker,
    mock_klio_config,
):
    mock_req_profile_input = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    config_data = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature",
            "project": "test-project",
            "region": "boonies",
            "staging_location": "gs://somewhere/over/the/rainbow",
            "temp_location": "gs://somewhere/over/the/rainbow",
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

    conf = mock_klio_config.setup(config_data, config_file, config_override)

    mock_pipeline = mocker.patch.object(
        cli.job_commands.profile, "ProfilePipeline"
    )

    exp_image_tag = image_tag or mock_get_git_sha.return_value
    if config_override:
        exp_image_tag = "{}-{}".format(exp_image_tag, config_override)

    exp_basic_config = cli.DockerRuntimeConfig(
        image_tag=exp_image_tag,
        force_build=False,
        config_file_override=config_override,
    )
    exp_prof_config = cli.ProfileConfig(
        input_file="input.txt",
        output_file="output.txt",
        show_logs=False,
        entity_ids=(),
    )

    kwargs = {
        "input_file": "input.txt",
        "output_file": "output.txt",
        "entity_ids": (),
        "force_build": False,
        "show_logs": False,
        "entity_ids": (),
    }

    cli._profile(
        subcommand="foo",
        klio_config=conf,
        image_tag=image_tag,
        config_meta=mock_klio_config.meta,
        **kwargs
    )

    mock_req_profile_input.assert_called_once_with("input.txt", ())
    exp_call = mocker.call(
        mock_klio_config.meta.job_dir, conf, exp_basic_config, exp_prof_config,
    )
    assert 1 == mock_pipeline.call_count
    assert exp_call == mock_pipeline.call_args
    mock_pipeline.return_value.run.assert_called_once_with(
        what="foo", subcommand_flags={}
    )


def test_profile_memory(runner, mocker, minimal_mock_klio_config):
    mock_profile = mocker.patch.object(cli, "_profile")

    result = runner.invoke(cli.profile_memory, [])

    core_testing.assert_execution_success(result)

    exp_kwargs = {
        "image_tag": None,
        "force_build": False,
        "include_children": False,
        "input_file": None,
        "interval": 0.1,
        "multiprocess": False,
        "output_file": None,
        "plot_graph": False,
        "show_logs": False,
        "entity_ids": (),
    }
    mock_profile.assert_called_once_with(
        "memory",
        minimal_mock_klio_config.klio_config,
        minimal_mock_klio_config.meta,
        **exp_kwargs
    )


def test_profile_memory_per_line(runner, mocker, minimal_mock_klio_config):
    mock_profile = mocker.patch.object(cli, "_profile")
    result = runner.invoke(cli.profile_memory_per_line, [])

    core_testing.assert_execution_success(result)

    exp_kwargs = {
        "get_maximum": False,
        "per_element": False,
        "image_tag": None,
        "force_build": False,
        "input_file": None,
        "output_file": None,
        "show_logs": False,
        "entity_ids": (),
    }
    mock_profile.assert_called_once_with(
        "memory-per-line",
        minimal_mock_klio_config.klio_config,
        minimal_mock_klio_config.meta,
        **exp_kwargs
    )


def test_profile_cpu(runner, mocker, minimal_mock_klio_config):
    mock_profile = mocker.patch.object(cli, "_profile")

    result = runner.invoke(cli.profile_cpu, [])

    core_testing.assert_execution_success(result)

    exp_kwargs = {
        "interval": 0.1,
        "plot_graph": False,
        "image_tag": None,
        "force_build": False,
        "input_file": None,
        "output_file": None,
        "show_logs": False,
        "entity_ids": (),
    }
    mock_profile.assert_called_once_with(
        "cpu",
        minimal_mock_klio_config.klio_config,
        minimal_mock_klio_config.meta,
        **exp_kwargs
    )


def test_profile_timeit(
    runner, mocker, mock_klio_config, minimal_mock_klio_config
):
    mock_profile = mocker.patch.object(cli, "_profile")

    result = runner.invoke(cli.profile_timeit, [])

    core_testing.assert_execution_success(result)

    exp_kwargs = {
        "iterations": 10,
        "image_tag": None,
        "force_build": False,
        "input_file": None,
        "output_file": None,
        "show_logs": False,
        "entity_ids": (),
    }
    mock_profile.assert_called_once_with(
        "timeit",
        mock_klio_config.klio_config,
        minimal_mock_klio_config.meta,
        **exp_kwargs
    )


@pytest.mark.parametrize(
    "force,ping,top_down,bottom_up,non_klio,conf_override",
    (
        (False, False, False, False, False, None),
        (False, False, True, False, False, None),
        (False, True, False, False, False, None),
        (True, False, False, False, False, None),
        (True, True, True, False, False, None),
        (False, False, False, False, True, None),
        (False, False, True, False, True, None),
        (False, False, False, False, False, "klio-job2.yaml"),
    ),
)
def test_publish(
    force,
    ping,
    top_down,
    bottom_up,
    non_klio,
    conf_override,
    runner,
    mocker,
    config_file,
    patch_os_getcwd,
    mock_klio_config,
):
    mock_publish = mocker.patch.object(
        cli.message_commands.publish, "publish_messages"
    )
    config = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature"
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

    cli_inputs = ["message", "publish", "deadb33f"]
    if force:
        cli_inputs.append("--force")
    if ping:
        cli_inputs.append("--ping")
    if top_down:
        cli_inputs.append("--top-down")
    if bottom_up:
        cli_inputs.append("--bottom-up")
    if non_klio:
        cli_inputs.append("--non-klio")
        config["job_config"]["allow_non_klio_messages"] = True
    if conf_override:
        cli_inputs.extend(["--config-file", conf_override])

    conf = mock_klio_config.setup(config, config_file)

    result = runner.invoke(cli.main, cli_inputs)

    core_testing.assert_execution_success(result)
    assert "" == result.output

    mock_publish.assert_called_once_with(
        conf, ("deadb33f",), force, ping, top_down, non_klio,
    )


def test_publish_raises_execution(
    runner, mocker, config_file, patch_os_getcwd, mock_klio_config
):
    mock_publish = mocker.patch.object(
        cli.message_commands.publish, "publish_messages"
    )

    cli_inputs = [
        "message",
        "publish",
        "deadb33f",
        "--top-down",
        "--bottom-up",
    ]

    config = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature"
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
    mock_klio_config.setup(config, config_file)

    result = runner.invoke(cli.main, cli_inputs)

    assert 0 != result.exit_code
    assert (
        "Error: Must either use `--top-down` or `--bottom-up`, not both."
        in result.output
    )

    mock_publish.assert_not_called()


@pytest.mark.parametrize(
    "force,ping,bottom_up",
    ((True, False, False), (False, True, False), (False, False, True)),
)
def test_publish_raises_non_klio(
    force, ping, bottom_up, runner, mocker, config_file, mock_klio_config
):
    mock_publish = mocker.patch.object(
        cli.message_commands.publish, "publish_messages"
    )
    config = {
        "job_name": "test-job",
        "pipeline_options": {
            "worker_harness_container_image": "gcr.register.io/squad/feature"
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

    mock_klio_config.setup(config, config_file)

    cli_inputs = ["message", "publish", "deadb33f", "--non-klio"]
    if force:
        cli_inputs.append("--force")
    if ping:
        cli_inputs.append("--ping")
    if bottom_up:
        cli_inputs.append("--bottom-up")

    result = runner.invoke(cli.main, cli_inputs)

    assert 0 != result.exit_code
    assert (
        "Can not publish a non-Klio message using --force, --ping, "
        "or --bottom-up flags." in result.output
    )

    mock_publish.assert_not_called()


@pytest.mark.parametrize("allow_non_klio_messages", (None, False))
def test_publish_raises_non_klio_config(
    runner,
    mocker,
    config_file,
    patch_os_getcwd,
    allow_non_klio_messages,
    mock_warn_if_py2_job,
    minimal_config_data,
):
    mock_publish = mocker.patch.object(
        cli.message_commands.publish, "publish_messages"
    )
    mock_get_config = mocker.patch.object(core_utils, "get_config_by_path")

    cli_inputs = ["message", "publish", "deadb33f", "--non-klio"]

    if allow_non_klio_messages is not None:
        minimal_config_data["job_config"][
            "allow_non_klio_messages"
        ] = allow_non_klio_messages

    mock_get_config.return_value = minimal_config_data
    result = runner.invoke(cli.main, cli_inputs)

    assert 1 == result.exit_code

    mock_get_config.assert_called_once_with(config_file)
    mock_publish.assert_not_called()

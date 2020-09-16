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

import os
from unittest import mock

import click
import pytest
import yaml

from click import testing

from klio_core import config as kconfig


def assert_execution_success(result):
    """Helper for testing CLI commands that emits errors if execution failed"""
    if result.exception:
        if result.stdout:
            print("captured stdout: {}".format(result.stdout))
        raise result.exception

    assert 0 == result.exit_code


@pytest.fixture
def mock_os_getcwd(monkeypatch):
    test_dir = "/test/dir"
    monkeypatch.setattr(os, "getcwd", lambda: test_dir)
    return test_dir


@pytest.fixture
def cli_runner():
    return testing.CliRunner()


def _config():
    return {
        "job_name": "klio-job-name",
        "job_config": {
            "inputs": [
                {
                    "data_location": "gs://test-in-data",
                    "topic": "test-in-topic",
                    "subscription": "test-sub",
                }
            ],
            "outputs": [
                {
                    "data_location": "gs://test-out-data",
                    "topic": "test-out-topic",
                }
            ],
        },
        "pipeline_options": {
            "streaming": True,
            "update": False,
            "worker_harness_container_image": "a-worker-image",
            "experiments": ["beam_fn_api"],
            "project": "test-gcp-project",
            "zone": "europe-west1-c",
            "region": "europe-west1",
            "staging_location": "gs://test-gcp-project-dataflow-tmp/staging",
            "temp_location": "gs://test-gcp-project-dataflow-tmp/temp",
            "max_num_workers": 2,
            "disk_size_gb": 32,
            "worker_machine_type": "n1-standard-2",
            "subnetwork": "some/path",
            "runner": "DataflowRunner",
        },
    }


@pytest.fixture
def config():
    return _config()


@pytest.fixture
def klio_config(config):
    return kconfig.KlioConfig(config)


# NOTE: Python decorators are evaluated on import, and so importing
# `klio_exec.commands.profile` (which imports `klio.transforms.helpers`, which
# imports `klio.transforms.decorators`) and `klio_exec.cli` triggers the  code
# in those decorators to get evaluated. Therefore, we must patch this part in
# order to import it, otherwise it will try to load the non-existant
# `/usr/src/config/.effective-klio-job.yaml`
mock_config = kconfig.KlioConfig(_config())
patcher = mock.patch(
    "klio.transforms.core.RunConfig.get", lambda: mock_config,
)
patcher.start()

from klio_exec.commands import profile  # noqa E402
from klio_exec import cli  # noqa E402


@pytest.fixture
def patch_get_config(monkeypatch, config):
    monkeypatch.setattr(cli, "_get_config", lambda x: config)


@pytest.fixture
def patch_klio_config(monkeypatch, klio_config):
    monkeypatch.setattr(cli.config, "KlioConfig", lambda x: klio_config)


@pytest.fixture
def patch_run_basic_pipeline(mocker, monkeypatch):
    mock = mocker.Mock()
    mocker.patch("klio_exec.commands.run.KlioPipeline.run", mock)
    return mock


def test_get_config(tmpdir, config):
    tmp_config = tmpdir.mkdir("klio-exec-testing").join("klio-job.yaml")
    tmp_config.write(yaml.dump(config))

    ret_config = cli._get_config(str(tmp_config))
    assert config == ret_config


def test_get_config_loaded_safely(tmpdir, config, mocker, monkeypatch):
    mock_safe_load = mocker.Mock()
    mock_safe_load.return_value = config
    monkeypatch.setattr(cli.yaml, "safe_load", mock_safe_load)
    tmp_config = tmpdir.mkdir("klio-exec-testing").join("klio-job.yaml")

    open_name = "klio_exec.cli.open"
    config_str = yaml.dump(config)
    m_open = mocker.mock_open(read_data=config_str)
    m = mocker.patch(open_name, m_open)

    cli._get_config(tmp_config.strpath)
    m.assert_called_once_with(tmp_config.strpath)
    mock_safe_load.assert_called_once_with(m_open.return_value)


def test_get_config_raises(tmpdir, caplog):
    tmp_config = tmpdir.mkdir("klio-exec-testing")

    does_not_exist = os.path.join(str(tmp_config), "klio-config.yaml")
    with pytest.raises(SystemExit):
        cli._get_config(does_not_exist)

    assert 1 == len(caplog.records)


@pytest.mark.parametrize("blocking", (True, False, None))
@pytest.mark.parametrize(
    "image_tag,direct_runner,update",
    (
        (None, True, False),
        (None, False, True),
        (None, False, None),
        ("a-tag", True, False),
        ("a-tag", False, False),
        ("a-tag", True, True),  # irrelevant, but CYA
    ),
)
def test_run_pipeline(
    image_tag,
    direct_runner,
    update,
    blocking,
    cli_runner,
    klio_config,
    patch_get_config,
    patch_run_basic_pipeline,
    patch_klio_config,
):
    runtime_conf = cli.RuntimeConfig(
        image_tag=None, direct_runner=False, update=None, blocking=None
    )
    cli_inputs = []
    if image_tag:
        cli_inputs.extend(["--image-tag", image_tag])
        runtime_conf = runtime_conf._replace(image_tag=image_tag)
    if direct_runner:
        cli_inputs.append("--direct-runner")
        runtime_conf = runtime_conf._replace(direct_runner=True)
    if update:
        cli_inputs.append("--update")
        runtime_conf = runtime_conf._replace(update=True)
    if update is False:
        cli_inputs.append("--no-update")
    if not update:  # if none or false
        runtime_conf = runtime_conf._replace(update=False)
    if blocking:
        cli_inputs.append("--blocking")
        runtime_conf._replace(blocking=True)
    if blocking is False:
        cli_inputs.append("--no-blocking")
    if not blocking:
        runtime_conf._replace(blocking=False)

    result = cli_runner.invoke(cli.run_pipeline, cli_inputs)
    assert 0 == result.exit_code

    patch_run_basic_pipeline.assert_called_once_with()


@pytest.mark.parametrize(
    "config_file_override", (None, "klio-job2.yaml"),
)
def test_run_pipeline_conf_override(
    config_file_override,
    cli_runner,
    config,
    klio_config,
    patch_get_config,
    patch_run_basic_pipeline,
    patch_klio_config,
    caplog,
    tmpdir,
    monkeypatch,
):

    cli_inputs = []

    temp_dir = tmpdir.mkdir("testing")
    temp_dir_str = str(temp_dir)
    monkeypatch.setattr(os, "getcwd", lambda: temp_dir_str)

    exp_conf_file = "klio-job.yaml"
    if config_file_override:
        exp_conf_file = os.path.join(temp_dir_str, config_file_override)
        cli_inputs.extend(["--config-file", exp_conf_file])

        # create a tmp file else click will complain it doesn't exist
        with open(exp_conf_file, "w") as f:
            yaml.dump(config, f)

    result = cli_runner.invoke(cli.run_pipeline, cli_inputs)
    assert 0 == result.exit_code

    patch_run_basic_pipeline.assert_called_once_with()

    assert 0 == len(caplog.records)


@pytest.mark.parametrize("config_file_override", (None, "klio-job2.yaml"))
def test_stop_job(
    config_file_override,
    mocker,
    monkeypatch,
    config,
    klio_config,
    patch_klio_config,
    cli_runner,
    tmpdir,
):
    mock_stop = mocker.Mock()
    monkeypatch.setattr(cli.stop, "stop", mock_stop)

    mock_get_config = mocker.Mock()
    monkeypatch.setattr(cli, "_get_config", mock_get_config)

    cli_inputs = []

    temp_dir = tmpdir.mkdir("testing")
    temp_dir_str = str(temp_dir)
    monkeypatch.setattr(os, "getcwd", lambda: temp_dir_str)

    exp_file = os.path.join(temp_dir_str, "klio-job.yaml")
    if config_file_override:
        # create a tmp file else click will complain it doesn't exist
        exp_file = os.path.join(temp_dir_str, config_file_override)
        cli_inputs.extend(["--config-file", exp_file])
        with open(exp_file, "w") as f:
            yaml.dump(config, f)

    result = cli_runner.invoke(cli.stop_job, cli_inputs)
    assert 0 == result.exit_code

    mock_stop.assert_called_once_with(klio_config, "cancel")
    mock_get_config.assert_called_once_with(exp_file)


@pytest.mark.parametrize(
    "pytest_args",
    [
        [],
        ["test_file.py::test_foo"],
        ["-s -h"],
        ["test_file.py::test_foo test:file_.py::test_foo2 -s"],
    ],
)
def test_test_job(
    pytest_args,
    mocker,
    monkeypatch,
    cli_runner,
    patch_get_config,
    patch_klio_config,
):
    mock_test = mocker.Mock()
    monkeypatch.setattr(pytest, "main", mock_test)
    mock_test.return_value = 0

    result = cli_runner.invoke(cli.test_job, pytest_args)

    assert_execution_success(result)
    assert "true" == os.environ["KLIO_TEST_MODE"]

    mock_test.assert_called_once_with(pytest_args)


@pytest.mark.parametrize(
    "pytest_args",
    [
        [],
        ["test_file.py::test_foo"],
        ["-s -h"],
        ["test_file.py::test_foo test:file_.py::test_foo2 -s"],
    ],
)
def test_test_job_raises(
    pytest_args,
    mocker,
    monkeypatch,
    cli_runner,
    patch_get_config,
    patch_klio_config,
):
    mock_test = mocker.Mock()
    mock_test.return_value = 1
    monkeypatch.setattr(pytest, "main", mock_test)

    result = cli_runner.invoke(cli.test_job, pytest_args)

    assert 1 == result.exit_code

    assert "true" == os.environ["KLIO_TEST_MODE"]
    mock_test.assert_called_once_with(pytest_args)


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


@pytest.mark.parametrize("input_file", (None, "input-data.txt"))
@pytest.mark.parametrize("show_logs", (True, False))
def test_run_profile_pipeline(
    input_file,
    show_logs,
    cli_runner,
    mocker,
    patch_klio_config,
    patch_get_config,
    klio_config,
):
    mock_req_prof_input_data = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    mock_klio_pipeline = mocker.patch.object(profile, "KlioPipeline")
    mock_logging_disable = mocker.patch.object(cli.logging, "disable")

    cli_inputs = ["run-pipeline"]
    if show_logs:
        cli_inputs.append("--show-logs")

    with cli_runner.isolated_filesystem():
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")
            cli_inputs.extend(["--input-file", input_file])

        result = cli_runner.invoke(cli.profile_job, cli_inputs)

    assert 0 == result.exit_code

    mock_req_prof_input_data.assert_called_once_with(input_file, ())
    mock_klio_pipeline.assert_called_once_with(
        input_file=input_file, entity_ids=(), klio_config=klio_config
    )
    mock_klio_pipeline.return_value.profile.assert_called_once_with(what="run")

    if not show_logs:
        mock_logging_disable.assert_called_once_with(cli.logging.CRITICAL)
    else:
        mock_logging_disable.assert_not_called()


@pytest.mark.parametrize(
    "interval,inc_child,multiproc", ((None, False, False), (0.5, True, True))
)
@pytest.mark.parametrize(
    "plot_graph,show_logs", ((True, True), (False, False))
)
@pytest.mark.parametrize(
    "input_file,output_file", ((None, None), ("input.txt", "output.txt"))
)
@pytest.mark.parametrize("entity_ids", (None, ("foo", "bar")))
def test_profile_memory(
    entity_ids,
    input_file,
    output_file,
    plot_graph,
    show_logs,
    interval,
    inc_child,
    multiproc,
    cli_runner,
    mocker,
    patch_klio_config,
    patch_get_config,
    klio_config,
):
    mock_req_prof_input_data = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    mock_klio_pipeline = mocker.patch.object(profile, "KlioPipeline")
    if not plot_graph:
        # it otherwise returns a mock, which evaluates to True
        mock_klio_pipeline.return_value.profile.return_value = None

    cli_inputs = ["memory"]
    if interval:
        cli_inputs.extend(["--interval", interval])
    if inc_child:
        cli_inputs.append("--include-children")
    if multiproc:
        cli_inputs.append("--multiprocess")
    if plot_graph:
        cli_inputs.append("--plot-graph")
    if show_logs:
        cli_inputs.append("--show-logs")
    if output_file:
        cli_inputs.extend(["--output-file", output_file])
    if entity_ids:
        cli_inputs.extend(entity_ids)

    with cli_runner.isolated_filesystem():
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")
            cli_inputs.extend(["--input-file", input_file])

        result = cli_runner.invoke(cli.profile_job, cli_inputs)

    assert 0 == result.exit_code

    mock_req_prof_input_data.assert_called_once_with(
        input_file, entity_ids or ()
    )
    mock_klio_pipeline.assert_called_once_with(
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids or (),
        klio_config=klio_config,
    )
    exp_kwargs = {
        "include_children": inc_child,
        "multiprocess": multiproc,
        "interval": interval or 0.1,
        "show_logs": show_logs,
        "plot_graph": plot_graph,
    }

    mock_klio_pipeline.return_value.profile.assert_called_once_with(
        what="memory", **exp_kwargs
    )
    if plot_graph:
        assert "Memory plot graph generated at" in result.output
    else:
        assert "" == result.output


@pytest.mark.parametrize("maximum,per_element", ((True, False), (False, True)))
@pytest.mark.parametrize("show_logs", (True, False))
@pytest.mark.parametrize(
    "input_file,output_file", ((None, None), ("input.txt", "output.txt"))
)
@pytest.mark.parametrize("entity_ids", (None, ("foo", "bar")))
def test_profile_memory_per_line(
    entity_ids,
    input_file,
    output_file,
    maximum,
    per_element,
    show_logs,
    cli_runner,
    mocker,
    patch_klio_config,
    patch_get_config,
    klio_config,
):
    mock_req_prof_input_data = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    mock_klio_pipeline = mocker.patch.object(profile, "KlioPipeline")
    mock_logging_disable = mocker.patch.object(cli.logging, "disable")

    cli_inputs = ["memory-per-line"]

    if maximum:
        cli_inputs.append("--maximum")
    if per_element:
        cli_inputs.append("--per-element")
    if show_logs:
        cli_inputs.append("--show-logs")
    if output_file:
        cli_inputs.extend(["--output-file", output_file])
    if entity_ids:
        cli_inputs.extend(entity_ids)

    with cli_runner.isolated_filesystem():
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")
            cli_inputs.extend(["--input-file", input_file])

        result = cli_runner.invoke(cli.profile_job, cli_inputs)

    assert 0 == result.exit_code

    if not show_logs:
        mock_logging_disable.assert_called_once_with(cli.logging.CRITICAL)
    else:
        mock_logging_disable.assert_not_called()

    mock_req_prof_input_data.assert_called_once_with(
        input_file, entity_ids or ()
    )
    mock_klio_pipeline.assert_called_once_with(
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids or (),
        klio_config=klio_config,
    )

    mock_klio_pipeline.return_value.profile.assert_called_once_with(
        what="memory_per_line", get_maximum=maximum
    )


@pytest.mark.parametrize(
    "interval,plot_graph,show_logs", ((None, True, True), (0.5, False, False))
)
@pytest.mark.parametrize(
    "input_file,output_file", ((None, None), ("input.txt", "output.txt"))
)
@pytest.mark.parametrize("entity_ids", (None, ("foo", "bar")))
def test_profile_cpu(
    entity_ids,
    input_file,
    output_file,
    plot_graph,
    show_logs,
    interval,
    cli_runner,
    mocker,
    patch_klio_config,
    patch_get_config,
    klio_config,
):
    mock_req_prof_input_data = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    mock_klio_pipeline = mocker.patch.object(profile, "KlioPipeline")
    if not plot_graph:
        # it otherwise returns a mock, which evaluates to True
        mock_klio_pipeline.return_value.profile.return_value = None

    cli_inputs = ["cpu"]
    if interval:
        cli_inputs.extend(["--interval", interval])
    if plot_graph:
        cli_inputs.append("--plot-graph")
    if show_logs:
        cli_inputs.append("--show-logs")
    if output_file:
        cli_inputs.extend(["--output-file", output_file])
    if entity_ids:
        cli_inputs.extend(entity_ids)

    with cli_runner.isolated_filesystem():
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")
            cli_inputs.extend(["--input-file", input_file])

        result = cli_runner.invoke(cli.profile_job, cli_inputs)

    assert 0 == result.exit_code

    mock_req_prof_input_data.assert_called_once_with(
        input_file, entity_ids or ()
    )
    mock_klio_pipeline.assert_called_once_with(
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids or (),
        klio_config=klio_config,
    )
    exp_kwargs = {
        "interval": interval or 0.1,
        "show_logs": show_logs,
        "plot_graph": plot_graph,
    }

    mock_klio_pipeline.return_value.profile.assert_called_once_with(
        what="cpu", **exp_kwargs
    )
    if plot_graph:
        assert "CPU plot graph generated at" in result.output
    else:
        assert "" == result.output


@pytest.mark.parametrize("iterations,show_logs", ((None, True), (5, False)))
@pytest.mark.parametrize(
    "input_file,output_file", ((None, None), ("input.txt", "output.txt"))
)
@pytest.mark.parametrize("entity_ids", (None, ("foo", "bar")))
def test_profile_wall_time(
    entity_ids,
    input_file,
    output_file,
    iterations,
    show_logs,
    cli_runner,
    mocker,
    patch_klio_config,
    patch_get_config,
    klio_config,
):
    mock_req_prof_input_data = mocker.patch.object(
        cli, "_require_profile_input_data"
    )
    mock_klio_pipeline = mocker.patch.object(profile, "KlioPipeline")
    mock_logging_disable = mocker.patch.object(cli.logging, "disable")

    cli_inputs = ["timeit"]

    if iterations:
        cli_inputs.extend(["--iterations", str(iterations)])
    if show_logs:
        cli_inputs.append("--show-logs")
    if output_file:
        cli_inputs.extend(["--output-file", output_file])
    if entity_ids:
        cli_inputs.extend(entity_ids)

    with cli_runner.isolated_filesystem():
        if input_file:
            with open(input_file, "w") as in_f:
                in_f.write("foo\n")
            cli_inputs.extend(["--input-file", input_file])

        result = cli_runner.invoke(cli.profile_job, cli_inputs)

    assert 0 == result.exit_code

    if not show_logs:
        mock_logging_disable.assert_called_once_with(cli.logging.CRITICAL)
    else:
        mock_logging_disable.assert_not_called()

    mock_req_prof_input_data.assert_called_once_with(
        input_file, entity_ids or ()
    )
    mock_klio_pipeline.assert_called_once_with(
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids or (),
        klio_config=klio_config,
    )

    exp_iterations = iterations or 10
    mock_klio_pipeline.return_value.profile.assert_called_once_with(
        what="timeit", iterations=exp_iterations
    )


@pytest.mark.parametrize("config_file", (None, "klio-job2.yaml"))
@pytest.mark.parametrize("list_steps", (True, False))
def test_audit_job(
    list_steps,
    config_file,
    patch_get_config,
    cli_runner,
    tmpdir,
    mocker,
    monkeypatch,
    mock_terminal_writer,
):
    test_dir = tmpdir.mkdir("testing")
    monkeypatch.setattr(os, "getcwd", lambda: str(test_dir))
    mock_audit = mocker.Mock()
    monkeypatch.setattr(cli.audit, "audit", mock_audit)
    mock_klio_config = mocker.Mock()
    monkeypatch.setattr(cli.config, "KlioConfig", mock_klio_config)

    # mocking out what audit.list_audit_steps calls, rather than the func
    # itself since it is a little difficult to patch directly (it's a
    # callback in a decorator, where it gets evaluated at import time, not
    # runtime)
    mock_print_plugins = mocker.Mock()
    monkeypatch.setattr(
        cli.audit.plugin_utils, "print_plugins", mock_print_plugins
    )

    cli_inputs = []
    if list_steps:
        cli_inputs.append("--list")
    if config_file:
        path = test_dir.join(config_file)
        path.write("foo")
        cli_inputs.extend(["--config-file", str(path)])

    ret = cli_runner.invoke(cli.audit_job, cli_inputs)

    assert 0 == ret.exit_code
    if not list_steps:
        mock_terminal_writer.sep.assert_not_called()
        mock_print_plugins.assert_not_called()
        assert "true" == os.environ["KLIO_TEST_MODE"]
        mock_audit.assert_called_once_with(
            str(test_dir), mock_klio_config.return_value
        )
    else:
        # call args are already tested in test_audit.py::test_list_audit_steps
        assert 1 == mock_terminal_writer.sep.call_count
        # difficult to actually patch the click context that's passed into the
        # callback; let's just care that it's actually called for now
        assert 1 == mock_print_plugins.call_count
        mock_audit.assert_not_called()

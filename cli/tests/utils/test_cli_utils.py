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

import subprocess

import pytest

from klio_cli.utils import cli_utils


@pytest.mark.parametrize("job_dir", (None, "/foo/bar/klio-job.yaml"))
def test_get_git_sha(mocker, job_dir):
    check_output_mock = mocker.patch.object(subprocess, "check_output")

    check_output_mock.return_value = b""

    result = cli_utils.get_git_sha(job_dir)

    cmd = "git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty"

    check_output_mock.assert_called_once_with(
        cmd.split(), cwd=job_dir, stderr=subprocess.DEVNULL
    )

    assert "" == result


def test_get_git_sha_called_process_error(mocker):
    check_output_mock = mocker.patch.object(subprocess, "check_output")

    check_output_mock.side_effect = subprocess.CalledProcessError(1, "")

    with pytest.raises(SystemExit):
        cli_utils.get_git_sha()


@pytest.mark.parametrize(
    "pipeline_options,will_raise",
    [
        ({}, True),
        (
            {  # missing one key
                "project": "p",
                "staging_location": "s",
                "region": "r",
            },
            True,
        ),
        (
            {  # has all keys
                "project": "p",
                "staging_location": "s",
                "temp_location": "t",
                "region": "r",
            },
            False,
        ),
    ],
)
def test_validate_dataflow_runner_config(
    mocker, caplog, pipeline_options, will_raise
):
    mock_klio_cfg = mocker.Mock()
    mock_klio_cfg.pipeline_options.as_dict.return_value = pipeline_options

    if will_raise:
        with pytest.raises(SystemExit):
            cli_utils.validate_dataflow_runner_config(mock_klio_cfg)

        assert 1 == len(caplog.records)
        assert "ERROR" == caplog.records[0].levelname
    else:
        cli_utils.validate_dataflow_runner_config(mock_klio_cfg)


@pytest.mark.parametrize(
    "config_runner,direct_runner,exp_is_direct,exp_mock_validate_df",
    (
        ("direct", True, True, False),
        ("direct", False, True, False),
        ("DirectRunner", True, True, False),
        ("DirectRunner", False, True, False),
        ("dataflow", True, True, False),
        ("dataflow", False, False, True),
        ("DataflowRunner", True, True, False),
        ("DataflowRunner", False, False, True),
        ("DirectGKERunner", True, True, False),
        ("DirectGKERunner", False, False, False),
    ),
)
def test_is_direct_runner(
    mocker,
    monkeypatch,
    config_runner,
    direct_runner,
    exp_is_direct,
    exp_mock_validate_df,
):
    mock_klio_cfg = mocker.Mock()
    mock_klio_cfg.pipeline_options.runner = config_runner
    mock_validate_df_config = mocker.Mock()
    monkeypatch.setattr(
        cli_utils, "validate_dataflow_runner_config", mock_validate_df_config
    )

    act_resp = cli_utils.is_direct_runner(mock_klio_cfg, direct_runner)

    assert exp_is_direct == act_resp
    if exp_mock_validate_df:
        mock_validate_df_config.assert_called_once_with(mock_klio_cfg)


@pytest.mark.parametrize(
    "sd_config",
    (
        {"stackdriver_logger": True},
        {"stackdriver_logger": {"timer_unit": "s"}},
    ),
)
def test_error_stackdriver_logger_metrics_raises(sd_config, mocker, caplog):
    mock_klio_config = mocker.Mock()
    mock_klio_config.pipeline_options.runner = "DataflowRunner"
    mock_klio_config.job_config.metrics = {"stackdriver_logger": sd_config}

    with pytest.raises(SystemExit):
        cli_utils.error_stackdriver_logger_metrics(mock_klio_config, False)

    assert 1 == len(caplog.records)
    assert (
        "The Stackdriver log-based metric client has been deprecated"
        in caplog.records[0].message
    )


@pytest.mark.parametrize(
    "metrics_conf",
    ({"stackdriver_logger": False}, {"native": {"timer_unit": "s"}}, {},),
)
def test_error_stackdriver_logger_metrics(metrics_conf, mocker, caplog):
    mock_klio_config = mocker.Mock()
    mock_klio_config.pipeline_options.runner = "DataflowRunner"
    mock_klio_config.job_config.metrics = metrics_conf

    cli_utils.error_stackdriver_logger_metrics(mock_klio_config, False)

    assert 0 == len(caplog.records)

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

import os
import subprocess

from unittest import mock

import pytest

from klio_cli.utils import cli_utils


@pytest.mark.parametrize(
    "image",
    (
        "dataflow.gcr.io/v1beta3/python",
        "dataflow.gcr.io/v1beta3/python-base",
        "dataflow.gcr.io/v1beta3/python-fnapi",
    ),
)
def test_warn_if_py2_job(image, patch_os_getcwd, mocker):
    dockerfile = (
        '## -*- docker-image-name: "gcr.io/foo/bar" -*-\n'
        "FROM {image}:1.2.3\n"
        'LABEL maintainer "me@example.com"\n'
    ).format(image=image)

    m_open = mock.mock_open(read_data=dockerfile)
    mock_open = mocker.patch("klio_cli.utils.cli_utils.open", m_open)

    warn_msg = (
        "Python 2 support in Klio is deprecated. "
        "Please upgrade to Python 3.5+"
    )
    with pytest.warns(UserWarning, match=warn_msg):
        cli_utils.warn_if_py2_job(patch_os_getcwd)

    exp_read_file = os.path.join(patch_os_getcwd, "Dockerfile")
    mock_open.assert_called_once_with(exp_read_file, "r")


@pytest.mark.parametrize("has_from_line", (True, False))
def test_warn_if_py2_job_no_warn(has_from_line, patch_os_getcwd, mocker):
    from_line = "\n"
    if has_from_line:
        from_line = "FROM dataflow.gcr.io/v1beta3/python36-fnapi:1.2.3\n"

    dockerfile = (
        '## -*- docker-image-name: "gcr.io/foo/bar" -*-\n'
        + from_line
        + 'LABEL maintainer "me@example.com"\n'
    )

    m_open = mock.mock_open(read_data=dockerfile)
    mock_open = mocker.patch("klio_cli.utils.cli_utils.open", m_open)

    cli_utils.warn_if_py2_job(patch_os_getcwd)

    exp_read_file = os.path.join(patch_os_getcwd, "Dockerfile")
    mock_open.assert_called_once_with(exp_read_file, "r")


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


@pytest.mark.parametrize("direct_runner", [False, True])
def test_is_direct_runner(mocker, monkeypatch, direct_runner):
    mock_klio_cfg = mocker.Mock()
    mock_validate_df_config = mocker.Mock()
    monkeypatch.setattr(
        cli_utils, "validate_dataflow_runner_config", mock_validate_df_config
    )

    if not direct_runner:
        assert mock_validate_df_config.called_once_with(mock_klio_cfg)

    assert (
        cli_utils.is_direct_runner(mock_klio_cfg, direct_runner)
        == direct_runner
    )


@pytest.mark.parametrize(
    "job_dir,conf_file",
    (
        (None, None),
        (None, "klio-job2.yaml"),
        ("foo/bar", None),
        ("foo/bar", "klio-job2.yaml"),
    ),
)
def test_get_config_job_dir(job_dir, conf_file, patch_os_getcwd):
    exp_job_dir = patch_os_getcwd
    if job_dir:
        exp_job_dir = os.path.abspath(os.path.join(patch_os_getcwd, job_dir))
    exp_conf_file = conf_file or os.path.join(exp_job_dir, "klio-job.yaml")
    if job_dir and conf_file:
        exp_conf_file = os.path.join(job_dir, conf_file)

    ret_job_dir, ret_conf_file = cli_utils.get_config_job_dir(
        job_dir, conf_file
    )

    assert exp_job_dir == ret_job_dir
    assert exp_conf_file == ret_conf_file

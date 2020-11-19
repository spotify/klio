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

import pytest

from google.api_core import exceptions as gapi_exceptions

from klio_core import utils


def test_set_global():
    utils.set_global("set-a-value", "a-value")

    actual = getattr(utils, "klio_global_state_set-a-value", None)
    assert "a-value" == actual

    delattr(utils, "klio_global_state_set-a-value")


def test_get_global():
    setattr(utils, "klio_global_state_get-a-value", "a-value")

    actual = utils.get_global("get-a-value")
    assert "a-value" == actual

    delattr(utils, "klio_global_state_get-a-value")


@pytest.mark.parametrize("exists", (True, False))
def test_delete_global(exists):
    if exists:
        setattr(utils, "klio_global_state_delete-a-value", "a-value")

    utils.delete_global("delete-a-value")

    actual = getattr(utils, "klio_global_state_delete-a-value", None)

    assert not actual


@pytest.mark.parametrize(
    "set_value,callable_init", ((True, False), (False, True), (False, False))
)
def test_get_or_initialize_global(set_value, callable_init, mocker):
    if set_value:
        setattr(utils, "klio_global_state_get-or-init-value", "a-value")
    if callable_init:
        initializer = mocker.Mock(return_value="a-value")
    else:
        initializer = "a-value"

    actual = utils.get_or_initialize_global("get-or-init-value", initializer)

    assert "a-value" == actual
    if not isinstance(initializer, str):
        initializer.assert_called_once_with()

    delattr(utils, "klio_global_state_get-or-init-value")


@pytest.fixture
def mock_publisher(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(utils.pubsub, "PublisherClient", mock)
    return mock


def test_private_get_publisher(mock_publisher):
    ret_publisher = utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    mock_publisher.return_value.create_topic.assert_called_once_with("a-topic")

    assert mock_publisher.return_value == ret_publisher


def test_private_get_publisher_topic_exists(mock_publisher):
    client = mock_publisher.return_value
    client.create_topic.side_effect = gapi_exceptions.AlreadyExists("foo")

    ret_publisher = utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.create_topic.assert_called_once_with("a-topic")

    assert client == ret_publisher


def test_private_get_publisher_raises(mock_publisher):
    client = mock_publisher.return_value
    client.create_topic.side_effect = Exception("foo")

    with pytest.raises(Exception, match="foo"):
        utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.create_topic.assert_called_once_with("a-topic")


@pytest.mark.parametrize("in_globals", (True, False))
def test_get_publisher(in_globals, mock_publisher):
    client = mock_publisher.return_value
    if in_globals:
        setattr(utils, "klio_global_state_publisher_a-topic", client)

    ret_publisher = utils.get_publisher("a-topic")

    assert client == ret_publisher
    delattr(utils, "klio_global_state_publisher_a-topic")


#########################
# Config Utils tests
#########################
@pytest.fixture
def patch_os_getcwd(monkeypatch, tmpdir):
    test_dir = str(tmpdir.mkdir("testing"))
    monkeypatch.setattr(os, "getcwd", lambda: test_dir)
    return test_dir


def test_get_config_by_path(mocker, monkeypatch):
    m_open = mocker.mock_open()
    mock_open = mocker.patch("klio_core.utils.open", m_open)

    mock_safe_load = mocker.Mock()
    monkeypatch.setattr(utils.yaml, "safe_load", mock_safe_load)

    path = "path/to/a/file"
    utils.get_config_by_path(path)

    mock_open.assert_called_once_with(path)


def test_get_config_by_path_error(mocker, monkeypatch, caplog):
    m_open = mocker.mock_open()
    mock_open = mocker.patch("klio_core.utils.open", m_open)
    mock_open.side_effect = IOError

    with pytest.raises(SystemExit):
        path = "path/to/a/file"
        utils.get_config_by_path(path)

    assert 1 == len(caplog.records)


#########################
# Cli/exec command tests
#########################
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
    mock_open = mocker.patch("klio_core.utils.open", m_open)
    m_is_file = mocker.Mock()
    m_is_file.return_value = True
    mock_is_file = mocker.patch("klio_core.utils.os.path.isfile", m_is_file)

    warn_msg = (
        "Python 2 support in Klio is deprecated. "
        "Please upgrade to Python 3.5+"
    )
    with pytest.warns(UserWarning, match=warn_msg):
        utils.warn_if_py2_job(patch_os_getcwd)

    exp_read_file = os.path.join(patch_os_getcwd, "Dockerfile")
    mock_is_file.assert_called_once_with(exp_read_file)
    mock_open.assert_called_once_with(exp_read_file, "r")


@pytest.mark.parametrize("has_from_line", (True, False))
@pytest.mark.parametrize("file_exists", (True, False))
def test_warn_if_py2_job_no_warn(
    has_from_line, file_exists, patch_os_getcwd, mocker
):
    from_line = "\n"
    if has_from_line:
        from_line = "FROM dataflow.gcr.io/v1beta3/python36-fnapi:1.2.3\n"

    dockerfile = (
        '## -*- docker-image-name: "gcr.io/foo/bar" -*-\n'
        + from_line
        + 'LABEL maintainer "me@example.com"\n'
    )

    m_open = mock.mock_open(read_data=dockerfile)
    mock_open = mocker.patch("klio_core.utils.open", m_open)
    m_is_file = mocker.Mock()
    m_is_file.return_value = file_exists
    mock_is_file = mocker.patch("klio_core.utils.os.path.isfile", m_is_file)

    utils.warn_if_py2_job(patch_os_getcwd)

    exp_read_file = os.path.join(patch_os_getcwd, "Dockerfile")
    mock_is_file.assert_called_once_with(exp_read_file)
    if file_exists:
        mock_open.assert_called_once_with(exp_read_file, "r")


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

    ret_job_dir, ret_conf_file = utils.get_config_job_dir(job_dir, conf_file)

    assert exp_job_dir == ret_job_dir
    assert exp_conf_file == ret_conf_file

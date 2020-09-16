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

import json
import os

import docker
import pytest

from docker import errors as docker_errors
from requests import exceptions as requests_exceptions

from klio_cli.utils import docker_utils


@pytest.fixture
def mock_docker_client(mocker):
    return mocker.Mock()


@pytest.fixture
def mock_json_loads(mocker):
    return mocker.patch.object(json, "loads")


@pytest.fixture
def mock_docker_api_client(mocker):
    return mocker.patch.object(docker, "APIClient")


@pytest.fixture
def mock_client(mocker):
    mock_client = mocker.Mock()
    return mock_client


@pytest.fixture
def mock_os_getcwd(monkeypatch):
    test_dir = "/test/dir"
    monkeypatch.setattr(os, "getcwd", lambda: test_dir)
    return "/test/dir"


@pytest.fixture
def mock_os_environ(mocker):
    return mocker.patch.dict("os.environ", {"HOME": "/home"})


def test_check_docker_connection(mock_docker_client):
    docker_utils.check_docker_connection(mock_docker_client)
    mock_docker_client.ping.assert_called_once_with()


@pytest.mark.parametrize(
    "error",
    [docker_errors.APIError("msg"), requests_exceptions.ConnectionError()],
)
def test_check_docker_connection_with_errors(
    mock_docker_client, error, caplog
):
    mock_docker_client.ping.side_effect = error
    with pytest.raises(SystemExit):
        docker_utils.check_docker_connection(mock_docker_client)

    assert 2 == len(caplog.records)


@pytest.mark.parametrize("path_exists", [True, False])
def test_check_dockerfile_present(monkeypatch, path_exists, caplog):
    monkeypatch.setattr(os.path, "exists", lambda x: path_exists)

    job_dir = "my/job/dir"

    if not path_exists:
        with pytest.raises(SystemExit):
            docker_utils.check_dockerfile_present(job_dir)
        assert 2 == len(caplog.records)
    else:
        docker_utils.check_dockerfile_present(job_dir)


def test_docker_image_exists(mocker, mock_client):
    mock_images = mocker.Mock()
    mock_images.get = mocker.Mock()

    images_mock = mocker.PropertyMock(return_value=mock_images)

    mock_client.images = images_mock

    image_tag = "gcr.io/sigint/test-image-name"

    exists = docker_utils.docker_image_exists(image_tag, mock_client)

    mock_client.images.get.assert_called_once_with(image_tag)
    assert exists


def test_docker_image_exists_with_image_not_found_error(mocker, mock_client):
    mock_images = mocker.Mock()
    mock_images.get = mocker.Mock()

    images_mock = mocker.PropertyMock(return_value=mock_images)

    mock_client.images = images_mock

    image_tag = "gcr.io/sigint/test-image-name"

    mock_client.images.get.side_effect = docker_errors.ImageNotFound(
        "pew", "pew"
    )

    exists = docker_utils.docker_image_exists(image_tag, mock_client)

    assert not exists


def test_docker_image_exists_with_api_error(mocker, mock_client, caplog):
    mock_images = mocker.Mock()
    mock_images.get = mocker.Mock()

    images_mock = mocker.PropertyMock(return_value=mock_images)

    mock_client.images = images_mock

    image_tag = "gcr.io/sigint/test-image-name"

    mock_client.images.get.side_effect = docker_errors.APIError("pew")

    with pytest.raises(SystemExit):
        docker_utils.docker_image_exists(image_tag, mock_client)

    assert 1 == len(caplog.records)


@pytest.mark.parametrize(
    "config_file,exp_config_file",
    ((None, "klio-job.yaml"), ("klio-job2.yaml", "klio-job2.yaml"),),
)
def test_build_docker_image(
    config_file,
    exp_config_file,
    mocker,
    mock_json_loads,
    mock_docker_api_client,
    caplog,
):
    mock_api_client = mocker.Mock()
    mock_docker_api_client.return_value = mock_api_client

    mock_api_client.build.return_value = (
        item for item in (b"BYTELOGS", "LOGS", "", '{"stream":"\\n"}')
    )

    mock_json_loads.return_value = {"stream": "SUCCESS"}

    job_dir = "/test/dir/jobs/test_run_job"
    image_name = "gcr.io/sigint/test-image-name"
    image_tag = "foobar"
    image_name_and_tag = "{}:{}".format(image_name, image_tag)

    build_flag = {
        "path": job_dir,
        "tag": image_name_and_tag,
        "rm": True,
        "buildargs": {"tag": image_tag, "KLIO_CONFIG": exp_config_file},
    }

    docker_utils.build_docker_image(
        job_dir, image_name, image_tag, config_file
    )

    docker.APIClient.assert_called_once_with(
        base_url="unix://var/run/docker.sock"
    )
    mock_api_client.build.assert_called_once_with(**build_flag)

    assert 2 == len(caplog.records)


def test_build_docker_image_with_errors(
    mocker, mock_json_loads, mock_docker_api_client, caplog
):
    mock_api_client = mocker.Mock()
    mock_docker_api_client.return_value = mock_api_client
    mock_api_client.build.return_value = "LOGS"

    mock_json_loads.return_value = {
        "error": "FAILURE",
        "errorDetail": {"message": "FAILURE"},
    }

    job_dir = "/test/dir/jobs/test_run_job"
    image_name = "gcr.io/sigint/test-image-name"
    image_tag = "foobar"
    image_name_and_tag = "{}:{}".format(image_name, image_tag)

    build_flag = {
        "path": job_dir,
        "tag": image_name_and_tag,
        "rm": True,
        "buildargs": {"tag": image_tag, "KLIO_CONFIG": "klio-job.yaml"},
    }

    with pytest.raises(SystemExit):
        docker_utils.build_docker_image(job_dir, image_name, image_tag)

    docker.APIClient.assert_called_once_with(
        base_url="unix://var/run/docker.sock"
    )
    mock_api_client.build.assert_called_once_with(**build_flag)

    assert 3 == len(caplog.records)


def test_push_image_to_gcr(mocker, capsys):
    image_name = "my.img.repo"
    tag = "my-tag"
    mock_client = mocker.Mock()
    mock_push = mocker.Mock()
    mock_client.images.push = mock_push
    mock_push.return_value = [
        b'{"status": "foo", "progress": {}}',
        b'{"id": "layerid1", "status": "bar", "progress": "some progress"}',
        (
            b'{"id": "layerid1", "status": "baz", "progress": "some more '
            b'progress"}\r\n{"id": "layerid2", "status": "foo", "progress": '
            b'"some other progress"}'
        ),
    ]

    exp_stdout = (
        "foo{}\nlayerid1: barsome progress\n\x1b[1Flayerid1: "
        "bazsome more progress\u001b[0K\nlayerid2: foosome other progress\n"
    )

    docker_utils.push_image_to_gcr(image_name, tag, mock_client)
    mock_push.assert_called_once_with(
        repository=image_name, tag=tag, stream=True
    )
    captured = capsys.readouterr()
    assert exp_stdout == captured.out


@pytest.mark.parametrize(
    "docker_image_exists, docker_image_build_raises, check_docker, "
    "check_dockerfile, force_build",
    [
        (True, False, False, False, True),
        (True, False, False, False, False),
        (False, False, False, False, False),
        (False, True, False, False, True),
        (False, True, False, False, False),
        (False, False, True, False, False),
        (False, False, False, True, False),
    ],
)
def test_get_docker_image_client(
    mocker,
    docker_image_exists,
    docker_image_build_raises,
    check_docker,
    check_dockerfile,
    force_build,
    caplog,
):
    mock_docker_from_env = mocker.patch.object(docker, "from_env")
    mock_check_docker_connection = mocker.patch.object(
        docker_utils, "check_docker_connection"
    )
    mock_check_dockerfile_present = mocker.patch.object(
        docker_utils, "check_dockerfile_present"
    )
    mock_docker_image_exists = mocker.patch.object(
        docker_utils, "docker_image_exists"
    )
    mock_build_docker_image = mocker.patch.object(
        docker_utils, "build_docker_image"
    )

    job_dir = "/test/dir/jobs/test_run_job"
    image_name = "gcr.io/sigint/test-image-name"
    image_tag = "foobar"
    image_name_and_tag = "{}:{}".format(image_name, image_tag)
    client = 'A "Docker Client" object.'

    mock_docker_from_env.return_value = client
    mock_docker_image_exists.return_value = docker_image_exists

    if docker_image_build_raises or check_docker or check_dockerfile:
        mock_check_docker_connection.side_effect = (
            SystemExit(1) if check_docker else None
        )
        mock_check_dockerfile_present.side_effect = (
            SystemExit(1) if check_dockerfile else None
        )
        mock_build_docker_image.side_effect = (
            SystemExit(1) if docker_image_build_raises else None
        )
        with pytest.raises(SystemExit) as _exec:
            docker_utils.get_docker_image_client(
                job_dir, image_tag, image_name, force_build
            )
        assert 1 == _exec.value.code
    else:

        actual_image, actual_client = docker_utils.get_docker_image_client(
            job_dir, image_tag, image_name, force_build
        )
        mock_check_docker_connection.assert_called_once_with(client)
        mock_check_dockerfile_present.assert_called_once_with(job_dir)

        if not docker_image_exists or force_build:
            assert mock_build_docker_image.called
        else:
            assert 1 == len(caplog.records)

        assert image_name_and_tag == actual_image
        assert client == actual_client

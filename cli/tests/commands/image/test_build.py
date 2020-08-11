# -*- coding: utf-8 -*-
# Copyright 2020 Spotify AB

import pytest

from klio_core import config

from klio_cli.commands.image import build as build_image


@pytest.mark.parametrize(
    "conf_file,exp_image_tag",
    (
        (None, "v1"),
        ("klio-job2.yaml", "v1-klio-job2.yaml"),
        ("bar/klio-job2.yaml", "v1-klio-job2.yaml"),
    ),
)
def test_build(conf_file, exp_image_tag, mocker, monkeypatch):

    mock_docker = mocker.Mock()
    mock_client = mocker.Mock()
    mock_docker.from_env.return_value = mock_client
    monkeypatch.setattr(build_image, "docker", mock_docker)

    mock_docker_utils = mocker.Mock()
    monkeypatch.setattr(build_image, "docker_utils", mock_docker_utils)

    mock_config = {
        "job_name": "test-job",
        "version": 1,
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
    conf_obj = config.KlioConfig(mock_config)
    job_dir = "jerbs"
    image_tag = "v1"

    build_image.build(job_dir, conf_obj, conf_file, image_tag)

    mock_docker.from_env.assert_called_once_with()
    mock_docker_utils.check_docker_connection.assert_called_once_with(
        mock_client
    )

    mock_docker_utils.check_dockerfile_present.assert_called_once_with(job_dir)

    mock_docker_utils.build_docker_image.assert_called_once_with(
        job_dir,
        conf_obj.pipeline_options.worker_harness_container_image,
        exp_image_tag,
        conf_file,
    )

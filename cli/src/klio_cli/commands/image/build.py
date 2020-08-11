# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB
from __future__ import absolute_import

import os

import docker

from klio_cli.utils import docker_utils


def build(job_dir, conf, config_file, image_tag):
    image_name = conf.pipeline_options.worker_harness_container_image
    client = docker.from_env()

    if config_file:
        basename = os.path.basename(config_file)
        image_tag = "{}-{}".format(image_tag, basename)

    docker_utils.check_docker_connection(client)
    docker_utils.check_dockerfile_present(job_dir)
    docker_utils.build_docker_image(
        job_dir, image_name, image_tag, config_file
    )

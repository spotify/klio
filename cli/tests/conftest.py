# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import logging
import os

import pytest


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def pipeline_config_dict():
    return {
        "project": "test-project",
        "staging_location": "gs://some/stage",
        "temp_location": "gs://some/temp",
        "worker_harness_container_image": "gcr.io/sigint/foo",
        "streaming": True,
        "update": False,
        "experiments": ["beam_fn_api"],
        "region": "us-central1",
        "num_workers": 3,
        "max_num_workers": 5,
        "disk_size_gb": 50,
        "worker_machine_type": "n1-standard-4",
    }


@pytest.fixture
def patch_os_getcwd(monkeypatch, tmpdir):
    test_dir = str(tmpdir.mkdir("testing"))
    monkeypatch.setattr(os, "getcwd", lambda: test_dir)
    return test_dir

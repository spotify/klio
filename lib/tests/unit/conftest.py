# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import logging

import pytest

from klio_core import config


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def job_config_dict():
    return {
        "metrics": {"logger": {}},
        "allow_non_klio_messages": False,
        "number_of_retries": 3,
        "inputs": [
            {
                "topic": "test-parent-job-out",
                "subscription": "test-parent-job-out-sub",
                "data_location": "gs://sigint-output/test-parent-job-out",
            }
        ],
        "outputs": [
            {
                "topic": "test-job-out",
                "data_location": "gs://sigint-output/test-job-out",
            }
        ],
        "dependencies": [
            {"gcp_project": "sigint", "job_name": "test-parent-job"}
        ],
        "more": "config",
        "that": {"the": "user"},
        "might": ["include"],
    }


@pytest.fixture
def pipeline_config_dict():
    return {
        "project": "test-project",
        "staging_location": "gs://some/stage",
        "temp_location": "gs://some/temp",
        "worker_harness_container_image": "gcr.io/sigint/foo",
        "streaming": True,
        "experiments": ["beam_fn_api"],
        "region": "us-central1",
        "num_workers": 3,
        "max_num_workers": 5,
        "disk_size_gb": 50,
        "worker_machine_type": "n1-standard-4",
        "runner": "direct",
        "autoscaling_algorithm": "THROUGHPUT_BASED",
        "update": False,
    }


@pytest.fixture
def config_dict(job_config_dict, pipeline_config_dict):
    return {
        "job_config": job_config_dict,
        "pipeline_options": pipeline_config_dict,
        "job_name": "test-job",
        "version": 1,
    }


@pytest.fixture
def klio_config(config_dict):
    return config.KlioConfig(config_dict)

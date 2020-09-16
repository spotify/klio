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


@pytest.fixture
def mock_config(mocker, monkeypatch):
    mconfig = mocker.Mock(name="MockKlioConfig")
    mconfig.job_name = "a-job"
    mconfig.pipeline_options.streaming = True
    mconfig.pipeline_options.project = "not-a-real-project"

    mock_data_input = mocker.Mock(name="MockDataGcsInput")
    mock_data_input.type = "gcs"
    mock_data_input.location = "gs://hopefully-this-bucket-doesnt-exist"
    mock_data_input.file_suffix = ""
    mock_data_input.skip_klio_existence_check = True
    mconfig.job_config.data.inputs = [mock_data_input]
    monkeypatch.setattr(
        "klio.transforms.core.RunConfig.get", lambda: mconfig,
    )
    return mconfig

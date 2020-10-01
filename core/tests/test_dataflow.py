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

import pytest

from klio_core import dataflow
from klio_core import utils


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "fixtures")


@pytest.fixture
def mock_discovery_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(dataflow.discovery, "build", lambda x, y: mock)
    return mock


def list_jobs():
    list_job_file = os.path.join(FIXTURE_PATH, "list_all_active_jobs.json")
    with open(list_job_file, "r") as f:
        return json.load(f)


def list_jobs_with_duplicates():
    list_job_file = os.path.join(
        FIXTURE_PATH, "list_all_active_jobs_duplicates.json"
    )
    with open(list_job_file, "r") as f:
        return json.load(f)


def get_job_detail():
    get_job_detail_file = os.path.join(FIXTURE_PATH, "get_job_detail.json")
    with open(get_job_detail_file, "r") as f:
        return json.load(f)


def get_job_detail_no_kind():
    return {"steps": [{"kind": "SomethingElse"}]}


def get_job_detail_no_username_value():
    return {
        "steps": [
            {
                "kind": "ParallelRead",
                "properties": {"username": "SomethingElse"},
            }
        ]
    }


def get_job_detail_no_topic():
    return {
        "steps": [
            {
                "kind": "ParallelRead",
                "properties": {"username": "ReadFromPubSub/Read"},
            }
        ]
    }


@pytest.mark.parametrize(
    "region,side_effect,exp_call_count,exp_log_count",
    (
        (None, [list_jobs_with_duplicates(), {}, {}], 13, 11),
        ("foo-region", [list_jobs(), {}, {}], 1, 0),
        (None, Exception("nojob4u"), 13, 13),
    ),
)
def test_find_job_by_name(
    region,
    side_effect,
    exp_call_count,
    exp_log_count,
    mock_discovery_client,
    caplog,
):
    client = dataflow.DataflowClient()
    locs = mock_discovery_client.projects.return_value.locations.return_value
    request = locs.jobs.return_value.list.return_value
    request.execute.side_effect = side_effect

    client.find_job_by_name("benchmark-beats", "gcp-project", region)

    assert exp_call_count == request.execute.call_count
    assert exp_log_count == len(caplog.records)


@pytest.mark.parametrize(
    "found_job,response,exp_log_count",
    (
        (True, Exception("foo"), 1),
        (True, get_job_detail(), 0),
        (False, None, 0),
    ),
)
def test_get_job_detail(
    found_job,
    response,
    exp_log_count,
    mock_discovery_client,
    caplog,
    mocker,
    monkeypatch,
):
    client = dataflow.DataflowClient()
    mock_find_job_by_name = mocker.Mock()
    monkeypatch.setattr(client, "find_job_by_name", mock_find_job_by_name)

    if found_job:
        mock_find_job_by_name.return_value = list_jobs().get("jobs")[0]
    else:
        mock_find_job_by_name.return_value = None

    locs = mock_discovery_client.projects.return_value.locations.return_value
    request = locs.jobs.return_value.get.return_value

    if isinstance(response, Exception):
        request.execute.side_effect = response
        exp_response = None
    else:
        request.execute.return_value = response
        exp_response = response

    ret_response = client.get_job_detail("job-name", "gcp-project")

    mock_find_job_by_name.assert_called_once_with(
        "job-name", "gcp-project", None
    )
    if found_job:
        assert 1 == request.execute.call_count
    assert exp_response == ret_response
    assert exp_log_count == len(caplog.records)


@pytest.mark.parametrize(
    "ret_job_info,exp_topic",
    (
        (lambda: None, None),
        (
            get_job_detail,
            "projects/sigint/topics/benchmark-audio-download-output",
        ),
        (get_job_detail_no_kind, None),
        (get_job_detail_no_username_value, None),
        (get_job_detail_no_topic, None),
    ),
)
def test_get_job_input_topic(
    ret_job_info, exp_topic, mock_discovery_client, mocker, monkeypatch
):
    client = dataflow.DataflowClient()
    mock_get_job_detail = mocker.Mock()
    monkeypatch.setattr(client, "get_job_detail", mock_get_job_detail)

    mock_get_job_detail.return_value = ret_job_info()

    ret_topic = client.get_job_input_topic("job-name", "gcp-project")

    assert exp_topic == ret_topic


@pytest.mark.parametrize(
    "api_version,exp_version", ((None, "v1b3"), ("v2", "v2"))
)
def test_get_dataflow_client(api_version, exp_version, mock_discovery_client):
    client = dataflow.get_dataflow_client(api_version)

    assert isinstance(client, dataflow.DataflowClient)
    global_client = utils.get_global("dataflow_client_%s" % exp_version)
    assert global_client == client

    delattr(utils, "klio_global_state_dataflow_client_%s" % exp_version)

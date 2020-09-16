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

import pytest

from googleapiclient import errors as gerrors

from klio_exec.commands import stop


@pytest.fixture
def mock_discovery_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(stop.discovery, "build", lambda x, y: mock)
    return mock


@pytest.fixture
def config(mocker):
    pipeline_options = mocker.Mock(
        project="test-project", region="europe-west1"
    )
    return mocker.Mock(job_name="test-job", pipeline_options=pipeline_options)


@pytest.fixture
def jobs_response():
    return {
        "jobs": [
            {"name": "not-the-test-job"},
            {
                "id": "1234",
                "name": "test-job",
                "projectId": "test-project",
                "location": "europe-west1",
            },
        ]
    }


@pytest.fixture
def job(jobs_response):
    return jobs_response["jobs"][1]


@pytest.fixture
def mock_sleep(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(stop.time, "sleep", mock)
    return mock


@pytest.mark.parametrize("api_version", (None, "v1b3", "v2"))
def test_set_dataflow_client(mock_discovery_client, api_version):
    assert stop._client is None

    stop._set_dataflow_client(api_version)

    assert stop._client is not None
    assert mock_discovery_client == stop._client

    # cleanup
    setattr(stop, "_client", None)


# return the desired job, no jobs at all, or no jobs matching job name
@pytest.mark.parametrize("returns_jobs", (True, False, None))
def test_check_job_running(
    mock_discovery_client, returns_jobs, jobs_response, config, monkeypatch
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.list

    req.return_value.execute.return_value = {}
    if returns_jobs:
        req.return_value.execute.return_value = jobs_response
    elif returns_jobs is False:
        req.return_value.execute.return_value = {
            "jobs": [{"name": "not-the-test-job"}]
        }

    ret = stop._check_job_running(config)

    if returns_jobs:
        assert jobs_response["jobs"][1] == ret
    else:
        assert ret is None

    req.assert_called_once_with(
        projectId="test-project", location="europe-west1", filter="ACTIVE"
    )
    req.return_value.execute.assert_called_once_with()


def test_check_job_running_errors(
    mock_discovery_client, config, monkeypatch, caplog
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.list
    req.return_value.execute.side_effect = Exception("foo")

    stop._check_job_running(config)

    req.assert_called_once_with(
        projectId="test-project", location="europe-west1", filter="ACTIVE"
    )
    req.return_value.execute.assert_called_once_with()
    assert 2 == len(caplog.records)


@pytest.mark.parametrize(
    "state,pyver", (("drain", None), ("cancel", None), (None, 2), (None, 3))
)
def test_update_job_state(
    state, pyver, mock_discovery_client, job, monkeypatch
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    exp_state = state
    if not state:
        if pyver == 2:
            exp_state = "JOB_STATE_DRAINED"
        else:
            exp_state = "JOB_STATE_CANCELLED"
        monkeypatch.setitem(stop.JOB_STATE_MAP, "default", exp_state)

    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update
    req.return_value.execute.return_value = None

    stop._update_job_state(job, state)

    job["requestedState"] = exp_state
    req.assert_called_once_with(
        jobId="1234",
        projectId="test-project",
        location="europe-west1",
        body=job,
    )
    req.return_value.execute.assert_called_once_with()


def test_update_job_state_400_error(
    mock_discovery_client, job, mock_sleep, mocker, monkeypatch, caplog
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update

    mock_resp = mocker.Mock(status=400)
    req.return_value.execute.side_effect = gerrors.HttpError(mock_resp, b"foo")

    with pytest.raises(SystemExit):
        stop._update_job_state(job, "drain")

    assert 1 == req.return_value.execute.call_count
    assert 1 == len(caplog.records)
    assert not mock_sleep.call_count


def test_update_job_state_500_error(
    mock_discovery_client, job, mock_sleep, mocker, monkeypatch, caplog
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update

    mock_resp = mocker.Mock(status=500)
    req.return_value.execute.side_effect = gerrors.HttpError(mock_resp, b"foo")

    with pytest.raises(SystemExit):
        stop._update_job_state(job, "drain")

    assert 4 == req.return_value.execute.call_count
    assert 4 == len(caplog.records)
    assert 3 == mock_sleep.call_count


def test_update_job_state_error(
    mock_discovery_client, job, mock_sleep, monkeypatch, caplog
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update
    req.return_value.execute.side_effect = Exception("foo")

    with pytest.raises(SystemExit):
        stop._update_job_state(job, "cancel")

    assert 4 == req.return_value.execute.call_count
    assert 4 == len(caplog.records)
    assert 3 == mock_sleep.call_count


@pytest.mark.parametrize(
    "exec_side_effect",
    (
        (
            {"currentState": "JOB_STATE_CANCELLING"},
            {"currentState": "JOB_STATE_CANCELLED"},
        ),
        (Exception("foo"), {"currentState": "JOB_STATE_CANCELLED"}),
    ),
)
def test_watch_job_state(
    mock_discovery_client,
    mock_sleep,
    monkeypatch,
    caplog,
    job,
    exec_side_effect,
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.get
    req.return_value.execute.side_effect = exec_side_effect

    stop._watch_job_state(job)

    assert 2 == req.return_value.execute.call_count
    mock_sleep.assert_called_once_with(5)
    assert 1 == len(caplog.records)


def test_watch_job_state_raises(
    mock_discovery_client, monkeypatch, caplog, job
):
    monkeypatch.setattr(stop, "_client", mock_discovery_client)

    with pytest.raises(SystemExit):
        stop._watch_job_state(job, timeout=0)

    assert 1 == len(caplog.records)


@pytest.mark.parametrize("has_running_job", (True, False))
def test_stop(has_running_job, config, mocker, monkeypatch, job):
    mock_set_dataflow_client = mocker.Mock()
    monkeypatch.setattr(stop, "_set_dataflow_client", mock_set_dataflow_client)

    ret_val = None
    if has_running_job:
        ret_val = job
    mock_check_job_running = mocker.Mock(return_value=ret_val)
    monkeypatch.setattr(stop, "_check_job_running", mock_check_job_running)
    mock_update_job_state = mocker.Mock()
    monkeypatch.setattr(stop, "_update_job_state", mock_update_job_state)
    mock_watch_job_state = mocker.Mock()
    monkeypatch.setattr(stop, "_watch_job_state", mock_watch_job_state)

    stop.stop(config, "cancel")

    mock_set_dataflow_client.assert_called_once_with()
    mock_check_job_running.assert_called_once_with(config)
    if has_running_job:
        mock_update_job_state.assert_called_once_with(job, req_state="cancel")
        mock_watch_job_state.assert_called_once_with(job)
    else:
        mock_update_job_state.assert_not_called()
        mock_watch_job_state.assert_not_called()

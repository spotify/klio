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

from klio_cli.commands.job import stop as stop_job


@pytest.fixture
def stop_job_inst():
    return stop_job.StopJob()


@pytest.fixture
def mock_discovery_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(stop_job.discovery, "build", lambda x, y: mock)
    return mock


@pytest.fixture
def job():
    return {
        "id": "1234",
        "name": "test-job",
        "projectId": "test-project",
        "location": "europe-west1",
    }


@pytest.fixture
def mock_sleep(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(stop_job.time, "sleep", mock)
    return mock


@pytest.mark.parametrize("api_version", (None, "v1b3", "v2"))
def test_set_dataflow_client(
    mock_discovery_client, api_version, stop_job_inst
):
    assert stop_job_inst._client is not None

    stop_job_inst._set_dataflow_client(api_version)

    assert stop_job_inst._client is not None
    assert mock_discovery_client == stop_job_inst._client

    # cleanup
    setattr(stop_job_inst, "_client", None)


@pytest.mark.parametrize(
    "jobs_response, result_is_none",
    (
        (
            {
                "jobs": [
                    {
                        "id": "1234",
                        "name": "test-job",
                        "projectId": "test-project",
                        "location": "europe-west1",
                    }
                ]
            },
            False,
        ),
        (
            {
                "jobs": [
                    {
                        "id": "1234",
                        "name": "not-this-job",
                        "projectId": "test-project",
                        "location": "europe-west1",
                    }
                ]
            },
            True,
        ),
        ({"jobs": []}, True),
    ),
)
def test_check_job_running(
    mock_discovery_client,
    jobs_response,
    result_is_none,
    monkeypatch,
    stop_job_inst,
    job,
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.list

    req.return_value.execute.return_value = jobs_response

    ret = stop_job_inst._check_job_running(
        job["name"], job["projectId"], job["location"]
    )

    exp_ret_val = jobs_response["jobs"][0] if not result_is_none else None

    assert exp_ret_val == ret

    req.assert_called_once_with(
        projectId="test-project", location="europe-west1", filter="ACTIVE"
    )
    req.return_value.execute.assert_called_once_with()


def test_check_job_running_errors(
    mock_discovery_client, monkeypatch, caplog, stop_job_inst, job
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.list
    req.return_value.execute.side_effect = Exception("foo")

    stop_job_inst._check_job_running(
        job["name"], job["projectId"], job["location"]
    )

    req.assert_called_once_with(
        projectId="test-project", location="europe-west1", filter="ACTIVE"
    )
    req.return_value.execute.assert_called_once_with()
    assert 2 == len(caplog.records)


@pytest.mark.parametrize(
    "state,pyver", (("drain", None), ("cancel", None), (None, 2), (None, 3))
)
def test_update_job_state(
    state, pyver, mock_discovery_client, job, monkeypatch, stop_job_inst
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    exp_state = state
    if not state:
        if pyver == 2:
            exp_state = "JOB_STATE_DRAINED"
        else:
            exp_state = "JOB_STATE_CANCELLED"
        monkeypatch.setitem(stop_job.JOB_STATE_MAP, "default", exp_state)

    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update
    req.return_value.execute.return_value = None

    stop_job_inst._update_job_state(job, state)

    job["requestedState"] = exp_state
    req.assert_called_once_with(
        jobId="1234",
        projectId="test-project",
        location="europe-west1",
        body=job,
    )
    req.return_value.execute.assert_called_once_with()


def test_update_job_state_400_error(
    mock_discovery_client,
    job,
    mock_sleep,
    mocker,
    monkeypatch,
    caplog,
    stop_job_inst,
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update

    mock_resp = mocker.Mock(status=400)
    req.return_value.execute.side_effect = gerrors.HttpError(mock_resp, b"foo")

    with pytest.raises(SystemExit):
        stop_job_inst._update_job_state(job, "drain")

    assert 1 == req.return_value.execute.call_count
    assert 1 == len(caplog.records)
    assert not mock_sleep.call_count


def test_update_job_state_500_error(
    mock_discovery_client,
    job,
    mock_sleep,
    mocker,
    monkeypatch,
    caplog,
    stop_job_inst,
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update

    mock_resp = mocker.Mock(status=500)
    req.return_value.execute.side_effect = gerrors.HttpError(mock_resp, b"foo")

    with pytest.raises(SystemExit):
        stop_job_inst._update_job_state(job, "drain")

    assert 4 == req.return_value.execute.call_count
    assert 4 == len(caplog.records)
    assert 3 == mock_sleep.call_count


def test_update_job_state_error(
    mock_discovery_client, job, mock_sleep, monkeypatch, caplog, stop_job_inst
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.update
    req.return_value.execute.side_effect = Exception("foo")

    with pytest.raises(SystemExit):
        stop_job_inst._update_job_state(job, "cancel")

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
    stop_job_inst,
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)
    _projects_req = mock_discovery_client.projects.return_value
    req = _projects_req.locations.return_value.jobs.return_value.get
    req.return_value.execute.side_effect = exec_side_effect

    stop_job_inst._watch_job_state(job)

    assert 2 == req.return_value.execute.call_count
    mock_sleep.assert_called_once_with(5)
    assert 1 == len(caplog.records)


def test_watch_job_state_raises(
    mock_discovery_client, monkeypatch, caplog, job, stop_job_inst
):
    monkeypatch.setattr(stop_job_inst, "_client", mock_discovery_client)

    with pytest.raises(SystemExit):
        stop_job_inst._watch_job_state(job, timeout=0)

    assert 1 == len(caplog.records)


def test_stop(mocker, monkeypatch, mock_discovery_client, job, stop_job_inst):
    mock_set_dataflow_client = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_set_dataflow_client", mock_set_dataflow_client
    )
    mock_check_job_running = mocker.Mock(return_value=job)
    monkeypatch.setattr(
        stop_job_inst, "_check_job_running", mock_check_job_running
    )
    mock_update_job_state = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_update_job_state", mock_update_job_state
    )
    mock_watch_job_state = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_watch_job_state", mock_watch_job_state
    )

    stop_job_inst.stop(
        job["name"], job["projectId"], job["location"], "cancel"
    )

    mock_set_dataflow_client.assert_called_once_with(None)
    mock_check_job_running.assert_called_once_with(
        "test-job", "test-project", "europe-west1"
    )
    mock_update_job_state.assert_called_once_with(job, req_state="cancel")
    mock_watch_job_state.assert_called_once_with(job)


def test_stop_no_running_job(
    mocker, monkeypatch, mock_discovery_client, stop_job_inst, job
):
    mock_set_dataflow_client = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_set_dataflow_client", mock_set_dataflow_client
    )
    mock_check_job_running = mocker.Mock(return_value=None)
    monkeypatch.setattr(
        stop_job_inst, "_check_job_running", mock_check_job_running
    )
    mock_update_job_state = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_update_job_state", mock_update_job_state
    )
    mock_watch_job_state = mocker.Mock()
    monkeypatch.setattr(
        stop_job_inst, "_watch_job_state", mock_watch_job_state
    )

    stop_job_inst.stop(
        job["name"], job["projectId"], job["location"], "cancel"
    )

    mock_set_dataflow_client.assert_called_once_with(None)
    mock_check_job_running.assert_called_once_with(
        "test-job", "test-project", "europe-west1"
    )
    mock_update_job_state.assert_not_called()
    mock_watch_job_state.assert_not_called()

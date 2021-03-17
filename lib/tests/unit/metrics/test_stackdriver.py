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

from googleapiclient import errors as gapi_errors

from klio.metrics import stackdriver as sd_metrics


BASE_WARN_MSG = " has been deprecated since `klio` version 21.3.0"
CLIENT_WARN_MSG = "StackdriverLogMetricsClient" + BASE_WARN_MSG
CTR_WARN_MSG = "StackdriverLogMetricsCounter" + BASE_WARN_MSG
GAUGE_WARN_MSG = "StackdriverLogMetricsGauge" + BASE_WARN_MSG
TIMER_WARN_MSG = "StackdriverLogMetricsTimer" + BASE_WARN_MSG


@pytest.fixture
def mock_stackdriver_client(mocker, monkeypatch):
    mock_client = mocker.Mock()
    monkeypatch.setattr(
        sd_metrics.discovery, "build", lambda x, y: mock_client
    )
    return mock_client


@pytest.fixture
def client(klio_config, monkeypatch, mocker, mock_stackdriver_client):
    return sd_metrics.StackdriverLogMetricsClient(klio_config)


@pytest.fixture
def mock_discovery_build(mocker, monkeypatch):
    mock_discovery_build = mocker.Mock()
    monkeypatch.setattr(sd_metrics.discovery, "build", mock_discovery_build)
    return mock_discovery_build


@pytest.fixture
def counter():
    kwargs = {
        "name": "my-counter",
        "job_name": "test-job",
        "project": "test-project",
    }
    return sd_metrics.StackdriverLogMetricsCounter(**kwargs)


@pytest.mark.filterwarnings(f"ignore:{CLIENT_WARN_MSG}")
def test_stackdriver_client(client, mock_discovery_build):
    # sanity check
    assert not getattr(client._thread_local, "stackdriver_client", None)

    client._stackdriver_client

    assert getattr(client._thread_local, "stackdriver_client", None)
    mock_discovery_build.assert_called_once_with("logging", "v2")


@pytest.mark.parametrize("value", (None, 5))
@pytest.mark.filterwarnings(f"ignore:{CLIENT_WARN_MSG}")
@pytest.mark.filterwarnings(f"ignore:{CTR_WARN_MSG}")
def test_client_counter(client, value, mocker, monkeypatch, caplog):
    mock_init_metric = mocker.Mock()
    monkeypatch.setattr(
        sd_metrics.StackdriverLogMetricsCounter,
        "_init_metric",
        mock_init_metric,
    )

    kwargs = {}
    if value:
        kwargs["value"] = value

    counter = client.counter(name="my-counter", **kwargs)

    mock_init_metric.assert_called_once_with(client._stackdriver_client)
    assert isinstance(counter, sd_metrics.StackdriverLogMetricsCounter)

    if value:
        assert 1 == len(caplog.records)
        assert "WARNING" == caplog.records[0].levelname


@pytest.mark.filterwarnings(f"ignore:{CLIENT_WARN_MSG}")
@pytest.mark.filterwarnings(f"ignore:{GAUGE_WARN_MSG}")
def test_client_gauge(client, caplog):
    gauge = client.gauge(name="my-gauge")

    assert isinstance(gauge, sd_metrics.StackdriverLogMetricsGauge)
    assert 1 == len(caplog.records)
    assert "WARNING" == caplog.records[0].levelname


@pytest.mark.filterwarnings(f"ignore:{CLIENT_WARN_MSG}")
@pytest.mark.filterwarnings(f"ignore:{TIMER_WARN_MSG}")
def test_client_timer(client, caplog):
    gauge = client.timer(name="my-timer")

    assert isinstance(gauge, sd_metrics.StackdriverLogMetricsTimer)
    assert 1 == len(caplog.records)
    assert "WARNING" == caplog.records[0].levelname


@pytest.mark.filterwarnings(f"ignore:{CTR_WARN_MSG}")
def test_counter_get_filter(counter):
    exp_filter = (
        'resource.type="dataflow_step" '
        'logName="projects/test-project/logs/'
        'dataflow.googleapis.com%2Fworker" '
        'jsonPayload.message:"[my-counter]"'
    )

    actual_filter = counter._get_filter()
    assert exp_filter == actual_filter


@pytest.mark.parametrize("raises", (False, True))
@pytest.mark.filterwarnings(f"ignore:{CTR_WARN_MSG}")
def test_counter_init_metric(raises, counter, mock_stackdriver_client, caplog):
    _mock_projects = mock_stackdriver_client.projects.return_value
    mock_req = _mock_projects.metrics.return_value.create
    if raises:
        mock_req.return_value.execute.side_effect = Exception("foo")

    counter._init_metric(mock_stackdriver_client)

    mock_req.assert_called_once_with(parent=counter.parent, body=counter.body)
    mock_req.return_value.execute.assert_called_once_with()

    if raises:
        assert 1 == len(caplog.records)
        assert "ERROR" == caplog.records[0].levelname


@pytest.mark.parametrize(
    "status,exp_log_level", ((409, "DEBUG"), (403, "ERROR"))
)
@pytest.mark.filterwarnings(f"ignore:{CTR_WARN_MSG}")
def test_counter_init_metric_api_error(
    status, exp_log_level, counter, mock_stackdriver_client, mocker, caplog
):
    mock_resp = mocker.Mock()
    mock_resp.status = status
    error = gapi_errors.HttpError(resp=mock_resp, content=b"foo")
    _mock_projects = mock_stackdriver_client.projects.return_value
    mock_req = _mock_projects.metrics.return_value.create
    mock_req.return_value.execute.side_effect = error

    counter._init_metric(mock_stackdriver_client)

    mock_req.assert_called_once_with(parent=counter.parent, body=counter.body)
    mock_req.return_value.execute.assert_called_once_with()

    assert 1 == len(caplog.records)
    assert exp_log_level == caplog.records[0].levelname

# Copyright 2021 Spotify AB
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
import shumway

from klio.metrics import shumway as kshumway


@pytest.fixture
def mock_shumway_client(mocker, monkeypatch):
    mock_client = mocker.create_autospec(shumway.MetricRelay, instance=True)
    monkeypatch.setattr(
        kshumway.shumway, "MetricRelay", lambda x, y: mock_client
    )
    return mock_client


@pytest.fixture
def client(klio_config, mock_shumway_client):
    return kshumway.ShumwayMetricsClient(klio_config)


@pytest.mark.parametrize(
    "transform,tags", ((None, {}), ("test-transform", {"foo": "bar"})),
)
def test_client_counter(transform, tags, client, mock_shumway_client):
    counter = client.counter(name="my-counter", transform=transform, tags=tags)

    assert isinstance(counter, kshumway.ShumwayCounter)
    assert "my-counter" == counter.name
    assert 0 == counter.value
    exp_attrs = {"transform": transform}
    exp_attrs.update(tags)
    assert exp_attrs == counter.attributes

    exp_attrs["job_name"] = "test-job"
    exp_unmarshalled = {
        "metric": "my-counter",
        "value": 0,
        "attributes": exp_attrs,
    }
    assert exp_unmarshalled == client.unmarshal(counter)
    client.emit(counter)
    mock_shumway_client.emit.assert_called_once_with(**exp_unmarshalled)


@pytest.mark.parametrize(
    "transform,tags", ((None, {}), ("test-transform", {"foo": "bar"})),
)
def test_client_gauge(transform, tags, client, mock_shumway_client):
    gauge = client.gauge(name="my-gauge", transform=transform, tags=tags)

    assert isinstance(gauge, kshumway.ShumwayGauge)
    assert "my-gauge" == gauge.name
    assert 0 == gauge.value
    exp_attrs = {"transform": transform}
    exp_attrs.update(tags)
    assert exp_attrs == gauge.attributes

    exp_attrs["job_name"] = "test-job"
    exp_unmarshalled = {
        "metric": "my-gauge",
        "value": 0,
        "attributes": exp_attrs,
    }
    assert exp_unmarshalled == client.unmarshal(gauge)
    client.emit(gauge)
    mock_shumway_client.emit.assert_called_once_with(**exp_unmarshalled)


@pytest.mark.parametrize(
    "transform,tags,timer_unit,exp_timer_unit",
    (
        (None, {}, None, "ns"),
        ("test-transform", {"foo": "bar"}, "seconds", "s"),
    ),
)
def test_client_timer(
    transform, tags, timer_unit, exp_timer_unit, client, mock_shumway_client
):
    timer = client.timer(
        name="my-timer", transform=transform, tags=tags, timer_unit=timer_unit
    )

    assert isinstance(timer, kshumway.ShumwayTimer)
    assert "my-timer" == timer.name
    assert 0 == timer.value
    exp_attrs = {"transform": transform, "unit": exp_timer_unit}
    exp_attrs.update(tags)
    assert exp_attrs == timer.attributes

    exp_attrs["job_name"] = "test-job"
    exp_unmarshalled = {
        "metric": "my-timer",
        "value": 0,
        "attributes": exp_attrs,
    }
    assert exp_unmarshalled == client.unmarshal(timer)
    client.emit(timer)
    mock_shumway_client.emit.assert_called_once_with(**exp_unmarshalled)


@pytest.mark.parametrize(
    "metrics_config,exp_timer_unit",
    (
        ({}, "ns"),
        ({"shumway": None}, "ns"),
        ({"shumway": {"timer_unit": "seconds"}}, "s"),
        ({"shumway": {"timer_unit": "ms"}}, "ms"),
    ),
)
def test_client_set_timer_unit(metrics_config, exp_timer_unit, klio_config):
    klio_config.job_config.metrics = metrics_config
    client = kshumway.ShumwayMetricsClient(klio_config)

    assert exp_timer_unit == client.timer_unit


@pytest.mark.parametrize("invalid_tag", ([], "foo", 123, set()))
def test_metric_creation_raises_assert(invalid_tag, client):
    exp_error_msg = "`tags` for metric objects should be dictionaries"
    with pytest.raises(AssertionError, match=exp_error_msg):
        client.counter(name="my-counter", tags=invalid_tag)
        client.gauge(name="my-gauge", tags=invalid_tag)
        client.timer(name="my-timer", tags=invalid_tag)

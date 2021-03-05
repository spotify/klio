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

from apache_beam import metrics as beam_metrics

from klio.metrics import native


@pytest.fixture
def client(klio_config):
    return native.NativeMetricsClient(klio_config)


@pytest.mark.parametrize(
    "transform,exp_namespace",
    ((None, "test-job"), ("test-transform", "test-transform")),
)
def test_client_counter(transform, exp_namespace, client, mocker, monkeypatch):
    counter = client.counter(name="my-counter", transform=transform)

    assert isinstance(counter, native.NativeCounter)
    assert isinstance(counter._counter, beam_metrics.metricbase.Counter)
    assert exp_namespace == counter._counter.metric_name.namespace
    assert "my-counter" == counter._counter.metric_name.name

    mock_inc = mocker.Mock()
    monkeypatch.setattr(counter._counter, "inc", mock_inc)
    counter.update(1)

    mock_inc.assert_called_once_with(1)


@pytest.mark.parametrize(
    "transform,exp_namespace",
    ((None, "test-job"), ("test-transform", "test-transform")),
)
def test_client_gauge(transform, exp_namespace, client, mocker, monkeypatch):
    gauge = client.gauge(name="my-gauge", transform=transform)

    assert isinstance(gauge, native.NativeGauge)
    assert isinstance(gauge._gauge, beam_metrics.metricbase.Gauge)
    assert exp_namespace == gauge._gauge.metric_name.namespace
    assert "my-gauge" == gauge._gauge.metric_name.name

    mock_set = mocker.Mock()
    monkeypatch.setattr(gauge._gauge, "set", mock_set)
    gauge.update(1)

    mock_set.assert_called_once_with(1)


@pytest.mark.parametrize(
    "timer_unit,exp_timer_unit",
    ((None, "s"), ("nanoseconds", "ns"), ("ms", "ms")),
)
@pytest.mark.parametrize(
    "transform,exp_namespace",
    ((None, "test-job"), ("test-transform", "test-transform")),
)
def test_client_timer(
    transform,
    exp_namespace,
    timer_unit,
    exp_timer_unit,
    client,
    mocker,
    monkeypatch,
):
    timer = client.timer(
        name="my-timer", transform=transform, timer_unit=timer_unit
    )

    assert isinstance(timer, native.NativeTimer)
    assert isinstance(timer._timer, beam_metrics.metricbase.Distribution)
    assert exp_namespace == timer._timer.metric_name.namespace
    assert "my-timer" == timer._timer.metric_name.name
    assert exp_timer_unit == timer.timer_unit

    mock_update = mocker.Mock()
    monkeypatch.setattr(timer._timer, "update", mock_update)
    timer.update(1)

    mock_update.assert_called_once_with(1)


# just to get 100% coverage for the module
def test_client_instance(client):
    assert "beam" == client.RELAY_CLIENT_NAME
    assert "s" == client.DEFAULT_TIME_UNIT

    assert client.unmarshal("metric") is None
    assert client.emit("metric") is None


@pytest.mark.parametrize(
    "metrics_config,exp_timer_unit",
    (
        ({}, "s"),
        ({"native": None}, "s"),
        ({"native": {"timer_unit": "nanoseconds"}}, "ns"),
        ({"native": {"timer_unit": "ms"}}, "ms"),
        ({"logger": {"timer_unit": "notaunit"}}, "s"),
    ),
)
def test_client_set_timer_unit(metrics_config, exp_timer_unit, klio_config):
    klio_config.job_config.metrics = metrics_config
    client = native.NativeMetricsClient(klio_config)

    assert exp_timer_unit == client.timer_unit

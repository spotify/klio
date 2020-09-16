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

from klio.metrics import dispatcher


class GenericDispatcher(dispatcher.BaseMetricDispatcher):

    METRIC_TYPE = "generic"

    def _setup_metric_relay(self, relay_clients):
        pass


@pytest.fixture
def metric_params():
    return {
        "name": "my-metric",
        "value": 0,
        "transform": "my-transform",
        "kwargs": {"tags": {"key-tag": "value-tag"}},
    }


@pytest.fixture
def metric(mocker):
    mock = mocker.Mock()
    mock.update = mocker.Mock()
    return mock


@pytest.fixture
def relay_client(mocker, metric):
    client = mocker.Mock()
    client.emit = mocker.Mock()
    client.counter.return_value = metric
    client.gauge.return_value = metric
    client.timer.return_value = metric
    return client


@pytest.fixture
def counter_dispatcher(
    relay_client, metric_params, metric, mocker, monkeypatch
):
    counter = dispatcher.CounterDispatcher(
        relay_clients=[relay_client], **metric_params
    )
    mock_submit_to_thread = mocker.Mock()
    monkeypatch.setattr(counter, "submit", mock_submit_to_thread)
    return counter


@pytest.fixture
def gauge_dispatcher(relay_client, metric_params, metric, mocker, monkeypatch):
    gauge = dispatcher.GaugeDispatcher(
        relay_clients=[relay_client], **metric_params
    )
    mock_submit_to_thread = mocker.Mock()
    monkeypatch.setattr(gauge, "submit", mock_submit_to_thread)
    return gauge


@pytest.fixture
def default_timer(mocker, monkeypatch):
    mock = mocker.Mock()
    mock.default_timer.side_effect = [0.0, 1.0]
    monkeypatch.setattr(dispatcher, "timeit", mock)
    return mock


@pytest.fixture
def timer_dispatcher(
    relay_client, metric_params, metric, default_timer, mocker, monkeypatch
):
    timer = dispatcher.TimerDispatcher(
        relay_clients=[relay_client], **metric_params
    )
    mock_submit_to_thread = mocker.Mock()
    monkeypatch.setattr(timer, "submit", mock_submit_to_thread)
    return timer


@pytest.fixture
def generic_dispatcher(relay_client, metric_params):
    return GenericDispatcher(relay_clients=[relay_client], **metric_params)


def test_base_metric_dispatcher_raises(relay_client, metric_params):
    class FakeMetricDispatcher(dispatcher.BaseMetricDispatcher):
        pass

    with pytest.raises(NotImplementedError):
        FakeMetricDispatcher(relay_clients=[relay_client], **metric_params)


@pytest.mark.parametrize(
    "has_transform,exp_key",
    ((True, "generic_my-metric_my-transform"), (False, "generic_my-metric")),
)
def test_base_metric_setup_metric_key(
    has_transform, exp_key, relay_client, metric_params
):
    if not has_transform:
        metric_params.pop("transform")

    gd = GenericDispatcher(relay_clients=[relay_client], **metric_params)
    assert exp_key == gd.metric_key


def test_base_metric_logger(generic_dispatcher):
    assert not getattr(
        generic_dispatcher._thread_local,
        "klio_metrics_dispatcher_logger",
        None,
    )  # sanity check

    logger = generic_dispatcher.logger
    assert logging.getLogger("klio.metrics.dispatcher") == logger
    assert (
        logger
        == generic_dispatcher._thread_local.klio_metrics_dispatcher_logger
    )


def test_base_metric_submit_callback(generic_dispatcher, mocker, caplog):
    fut = mocker.Mock()
    fut.result.return_value = None

    generic_dispatcher._submit_callback(fut)

    fut.result.assert_called_once_with()
    assert not len(caplog.records)


def test_base_metric_submit_callback_exception(
    generic_dispatcher, mocker, caplog
):
    fut = mocker.Mock()
    fut.result.side_effect = Exception("fuu")

    generic_dispatcher._submit_callback(fut)

    fut.result.assert_called_once_with()
    assert 1 == len(caplog.records)
    assert logging.WARNING == caplog.records[0].levelno


def test_base_metric_submit(
    generic_dispatcher, relay_client, mocker, monkeypatch
):
    mock_thread_pool, mock_future = mocker.Mock(), mocker.Mock()
    mock_thread_pool.submit.return_value = mock_future
    monkeypatch.setattr(generic_dispatcher, "_thread_pool", mock_thread_pool)

    metric = mocker.Mock()
    metric.name, metric.transform = "my-metric", "my-transform"
    expected_key = "generic_my-metric_my-transform"

    generic_dispatcher.submit(relay_client.emit, metric)

    mock_thread_pool.submit.assert_called_once_with(relay_client.emit, metric)
    assert expected_key == mock_future.metric_key
    mock_future.add_done_callback.assert_called_once_with(
        generic_dispatcher._submit_callback
    )


def test_counter_setup_metric_relay(relay_client, metric, metric_params):
    counter = dispatcher.CounterDispatcher(
        relay_clients=[relay_client], **metric_params
    )

    expected_relay_to_metric = [(relay_client, metric)]
    assert expected_relay_to_metric == counter.relay_to_metric
    relay_client.counter.assert_called_once_with(**metric_params)


def test_counter_inc(counter_dispatcher, relay_client, metric):
    assert 0 == counter_dispatcher.value  # sanity check

    counter_dispatcher.inc()

    assert 1 == counter_dispatcher.value
    metric.update.assert_called_once_with(1)
    counter_dispatcher.submit.assert_called_once_with(
        relay_client.emit, metric
    )


def test_gauge_setup_metric_relay(relay_client, metric, metric_params):
    gauge = dispatcher.GaugeDispatcher(
        relay_clients=[relay_client], **metric_params
    )

    expected_relay_to_metric = [(relay_client, metric)]
    assert expected_relay_to_metric == gauge.relay_to_metric
    relay_client.gauge.assert_called_once_with(**metric_params)


def test_gauge_set(gauge_dispatcher, relay_client, metric):
    assert 0 == gauge_dispatcher.value  # sanity check

    gauge_dispatcher.set(5)

    assert 5 == gauge_dispatcher.value
    metric.update.assert_called_once_with(5)
    gauge_dispatcher.submit.assert_called_once_with(relay_client.emit, metric)


def test_timer_setup_metric_relay(relay_client, metric, metric_params):
    metric_params["timer_unit"] = "ns"
    timer = dispatcher.TimerDispatcher(
        relay_clients=[relay_client], **metric_params
    )

    expected_relay_to_metric = [(relay_client, metric)]
    assert expected_relay_to_metric == timer.relay_to_metric
    relay_client.timer.assert_called_once_with(**metric_params)


def test_timer_start(timer_dispatcher, metric):
    assert 0 == timer_dispatcher.value  # sanity check
    assert not timer_dispatcher._start_time  # sanity check

    timer_dispatcher.start()

    assert 0.0 == timer_dispatcher._start_time
    metric.update.assert_not_called()
    timer_dispatcher.submit.assert_not_called()


def test_timer_stop(timer_dispatcher, relay_client, metric):
    assert 0 == timer_dispatcher.value  # sanity check
    assert not timer_dispatcher._start_time  # sanity check

    timer_dispatcher.start()

    assert 0.0 == timer_dispatcher._start_time
    metric.update.assert_not_called()
    timer_dispatcher.submit.assert_not_called()

    timer_dispatcher.stop()
    # seconds -> nanoseconds
    assert 1.0 * 1e9 == timer_dispatcher.value
    metric.update.assert_called_once_with(timer_dispatcher.value)
    timer_dispatcher.submit.assert_called_once_with(relay_client.emit, metric)


def test_timer_stop_no_start(timer_dispatcher, metric, caplog):
    assert not timer_dispatcher._start_time  # sanity check

    timer_dispatcher.stop()

    assert 1 == len(caplog.records)
    assert logging.WARNING == caplog.records[0].levelno

    metric.update.assert_not_called()
    timer_dispatcher.submit.assert_not_called()


def test_timer_context_manager(
    relay_client, metric, metric_params, default_timer, mocker, monkeypatch
):
    timer = dispatcher.TimerDispatcher(
        relay_clients=[relay_client], **metric_params
    )
    mock_submit_to_thread = mocker.Mock()
    monkeypatch.setattr(timer, "submit", mock_submit_to_thread)
    with timer:
        assert 1 == 1

    assert 0 == timer._start_time
    assert 1 * 1e9 == timer.value
    metric.update.assert_called_once_with(timer.value)
    timer.submit.assert_called_once_with(relay_client.emit, metric)

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
import threading

import pytest

from klio.metrics import logger


@pytest.fixture
def client(klio_config):
    return logger.MetricsLoggerClient(klio_config)


@pytest.fixture
def metric():
    return logger.LoggerCounter(name="fixture-counter")


@pytest.mark.parametrize(
    "metrics_config,exp_log_level",
    (
        ({}, logging.DEBUG),
        ({"logger": True}, logging.DEBUG),
        ({"logger": {"key": "value"}}, logging.DEBUG),
        ({"logger": {"level": "info"}}, logging.INFO),
        ({"logger": {"level": "INFO"}}, logging.INFO),
        ({"logger": {"level": "notalevel"}}, logging.DEBUG),
    ),
)
def test_client_set_log_level(metrics_config, exp_log_level, klio_config):
    klio_config.job_config.metrics = metrics_config
    client = logger.MetricsLoggerClient(klio_config)

    assert exp_log_level == client.log_level


@pytest.mark.parametrize(
    "metrics_config,exp_timer_unit",
    (
        ({}, "ns"),
        ({"logger": True}, "ns"),
        ({"logger": {"timer_unit": "seconds"}}, "s"),
        ({"logger": {"timer_unit": "notaunit"}}, "ns"),
    ),
)
def test_client_set_timer_unit(metrics_config, exp_timer_unit, klio_config):
    klio_config.job_config.metrics = metrics_config
    client = logger.MetricsLoggerClient(klio_config)

    assert exp_timer_unit == client.timer_unit


def test_client_logger(client):
    ret_logger = client.logger

    assert logging.getLogger("klio.metrics") == ret_logger


def test_client_logger_local(client, monkeypatch):
    logger_patch = logging.getLogger("klio.metrics.patch")
    thread_local_patch = threading.local()
    thread_local_patch.klio_metrics_logger = logger_patch
    monkeypatch.setattr(client, "_thread_local", thread_local_patch)

    ret_logger = client.logger

    assert logger_patch == ret_logger


def test_client_unmarshal(client, metric):
    exp_metric_dict = {
        "name": "fixture-counter",
        "value": 0,
        "transform": None,
        "tags": {"metric_type": "counter"},
    }
    unmarshalled = client.unmarshal(metric)
    assert exp_metric_dict == unmarshalled


def test_client_emit(client, metric, caplog):
    exp_log_record = (
        "[fixture-counter] value: 0 transform: 'None' "
        "tags: {'metric_type': 'counter'}"
    )
    client.emit(metric)

    assert 1 == len(caplog.records)
    assert exp_log_record == caplog.records[0].message


def test_client_counter(client):
    counter = client.counter(name="my-counter")
    assert isinstance(counter, logger.LoggerCounter)


def test_client_gauge(client):
    gauge = client.gauge(name="my-gauge")
    assert isinstance(gauge, logger.LoggerGauge)


@pytest.mark.parametrize(
    "timer_unit,config_timer_unit,exp_timer_unit",
    (
        (None, "ns", "ns"),
        (None, None, "ns"),
        ("s", None, "s"),
        ("microseconds", None, "us"),
        ("notaunit", None, "ns"),
    ),
)
def test_client_timer(
    timer_unit, config_timer_unit, exp_timer_unit, client, monkeypatch
):
    if config_timer_unit:
        monkeypatch.setattr(client, "timer_unit", config_timer_unit)
    timer = client.timer(name="my-timer", timer_unit=timer_unit)
    assert isinstance(timer, logger.LoggerTimer)
    assert exp_timer_unit == timer.tags["unit"]


def test_logger_counter():
    expected_tags = {"metric_type": "counter"}

    counter = logger.LoggerCounter(name="my-counter")
    assert expected_tags == counter.tags


def test_logger_gauge():
    expected_tags = {"metric_type": "gauge"}

    gauge = logger.LoggerGauge(name="my-gauge")
    assert expected_tags == gauge.tags


def test_logger_timer():
    expected_tags = {"metric_type": "timer", "unit": "ns"}
    timer = logger.LoggerTimer(name="my-timer")
    assert expected_tags == timer.tags

    expected_tags["unit"] = "s"
    timer = logger.LoggerTimer(name="my-timer", timer_unit="s")
    assert expected_tags == timer.tags


# pseudo-integration test
@pytest.mark.parametrize("disabled,exp", ((True, 0), (False, 1)))
def test_logger_disabled(disabled, exp, client, metric, caplog, monkeypatch):
    caplog.set_level(logging.DEBUG, logger="klio.metrics")
    # sanity check / clear out thread local
    client._thread_local.klio_metrics_logger = None

    monkeypatch.setattr(client, "disabled", disabled)

    client.emit(metric)

    assert exp == len(caplog.records)

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

from klio.metrics import client
from klio.metrics import dispatcher


@pytest.fixture
def metric_params():
    return {
        "name": "my-metric",
        "value": 0,
        "kwargs": {"tags": {"key-tag": "value-tag"}},
    }


@pytest.fixture
def relay_client(mocker):
    return mocker.Mock()


@pytest.fixture
def metrics_registry(relay_client):
    return client.MetricsRegistry(
        relay_clients=[relay_client], transform_name="HelloKlio"
    )


@pytest.fixture
def metric_data(metric_params):
    metric_params["type"] = "counter"
    return metric_params


@pytest.fixture
def counter_metric(metric_params, relay_client):
    return dispatcher.CounterDispatcher(
        relay_clients=[relay_client], **metric_params
    )


@pytest.mark.parametrize(
    "method,cls",
    (
        ("counter", dispatcher.CounterDispatcher),
        ("gauge", dispatcher.GaugeDispatcher),
        ("timer", dispatcher.TimerDispatcher),
    ),
)
def test_get_metric_inst(method, cls, metrics_registry, metric_params):
    assert {} == metrics_registry._registry  # sanity check

    method_to_call = getattr(metrics_registry, method)
    ret_metric = method_to_call(**metric_params)

    assert isinstance(ret_metric, cls)

    exp_key = "{method}_{name}_HelloKlio".format(
        method=method, **metric_params
    )
    assert exp_key in metrics_registry._registry
    assert ret_metric == metrics_registry._registry[exp_key]

    # assert same metric is returned rather than creating a new instance
    ret_metric_again = method_to_call(**metric_params)
    assert ret_metric is ret_metric_again


@pytest.mark.parametrize(
    "metric_type,cls",
    (
        ("counter", dispatcher.CounterDispatcher),
        ("gauge", dispatcher.GaugeDispatcher),
        ("timer", dispatcher.TimerDispatcher),
        ("unknown", dispatcher.GaugeDispatcher),
    ),
)
def test_marshal_unmarshal(
    metrics_registry, metric_type, cls, metric_params, relay_client
):
    metric_inst = cls(
        relay_clients=[relay_client], transform="HelloKlio", **metric_params
    )
    metric_data = metric_params.copy()
    metric_data["type"] = metric_type
    ret_metric_data = metrics_registry.marshal(metric_inst)

    exp_metric_data = metric_data.copy()
    if metric_type == "unknown":
        exp_metric_data["type"] = "gauge"
    if metric_type == "timer":
        exp_metric_data["timer_unit"] = "ns"
    assert exp_metric_data == ret_metric_data

    ret_metric = metrics_registry.unmarshal(metric_data)

    # can't simply compare exp_metric to ret_metric since they are
    # two different instances
    # FIXME: implement __eq__ and __ne__ for dispatch objects (@lynn)
    assert metric_inst.METRIC_TYPE == ret_metric.METRIC_TYPE
    assert metric_inst.name == ret_metric.name
    assert metric_inst.value == ret_metric.value
    assert metric_inst.transform == ret_metric.transform
    assert metric_inst.kwargs == ret_metric.kwargs

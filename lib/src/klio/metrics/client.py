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
"""Metrics registry to manage metrics for all configured relay clients.

The :class:`MetricsRegistry` class is the main client for which to create
and emit metrics. For example:

.. code-block:: python

    class MyTransform(KlioBaseTransform):
        def __init__(self):
            self.my_counter = self._klio.metrics.counter(
                name="foo",
                value=2,
                transform="mytransform",
                tags={"tag1": "value1"}
            )

        def process(self, element):
            # user code

            # immediately emits a metric
            self.my_counter.inc()

"""

import logging

from klio.metrics import dispatcher


class MetricsRegistry(object):
    """Main client to create and emit metrics.

    Args:
        relay_clients (list(klio.metrics.base.AbstractRelayClient)):
            configured relay clients.
    """

    def __init__(self, relay_clients, transform_name):
        self._relays = relay_clients
        self._transform_name = transform_name
        self._registry = {}

    def counter(self, name, value=0, **kwargs):
        """Get or create a counter.

        Creates a new counter if one is not found with a key of
        ``"counter_{name}_{transform}"`` of the given  ``transform``.

        New counters will be stored in memory for simple caching.

        Args:
            name (str): name of counter
            value (int): starting value of counter; defaults to 0
            kwargs (dict): keyword arguments passed to each configured
                relay clients' counter object.
        Returns:
            dispatcher.CounterDispatcher: instance of a counter dispatcher
        """
        transform_name = kwargs.pop("transform", self._transform_name)
        key = "counter_{}_{}".format(name, transform_name)
        if key in self._registry:
            return self._registry[key]

        counter = dispatcher.CounterDispatcher(
            relay_clients=self._relays,
            name=name,
            value=value,
            transform=transform_name,
            **kwargs
        )
        self._registry[key] = counter
        return counter

    def gauge(self, name, value=0, **kwargs):
        """Get or create a gauge.

        Creates a new gauge if one is not found with a key of
        "gauge_{name}_{transform}".

        New gauges will be stored in memory for simple caching.

        Args:
            name (str): name of gauge
            value (int): starting value of gauge; defaults to 0
            kwargs (dict): keyword arguments passed to each configured
                relay clients' gauge object.
        Returns:
            dispatcher.GaugeDispatcher: instance of a gauge dispatcher
        """
        transform_name = kwargs.pop("transform", self._transform_name)
        key = "gauge_{}_{}".format(name, transform_name)
        if key in self._registry:
            return self._registry[key]

        gauge = dispatcher.GaugeDispatcher(
            relay_clients=self._relays,
            name=name,
            value=value,
            transform=transform_name,
            **kwargs
        )
        self._registry[key] = gauge
        return gauge

    def timer(self, name, value=0, timer_unit="ns", **kwargs):
        """Get or create a timer.

        Creates a new timer if one is not found with a key of
        "timer_{name}_{transform}".

        New timers will be stored in memory for simple caching.

        Args:
            name (str): name of timer
            value (int): starting value of timer; defaults to 0
            timer_unit (str): desired unit of time; defaults to ns
            kwargs (dict): keyword arguments passed to each configured
                relay clients' timer object.
        Returns:
            dispatcher.TimerDispatcher: instance of a timer dispatcher
        """
        transform_name = kwargs.pop("transform", self._transform_name)
        key = "timer_{}_{}".format(name, transform_name)
        if key in self._registry:
            return self._registry[key]

        timer = dispatcher.TimerDispatcher(
            relay_clients=self._relays,
            name=name,
            value=value,
            transform=transform_name,
            timer_unit=timer_unit,
            **kwargs
        )
        self._registry[key] = timer
        return timer

    def marshal(self, metric):
        """Create a dictionary-representation of a given metric.

        Used when metric objects need to be pickled.

        Args:
            metric (dispatcher.BaseMetricDispatcher): metric instance to
                marshal.

        Returns:
            dict: the metric's data in dictionary form
        """
        data = {
            "type": metric.METRIC_TYPE,
            "name": metric.name,
            "value": metric.value,
        }
        data.update(metric.kwargs)
        return data

    def unmarshal(self, metric_data):
        """Create a metric instance based off of a dictionary.

        If "type" is not specified or is not one of "counter", "gauge",
        or "timer", it defaults to a gauge-type metric.

        Used when metrics objects need to be unpickled.

        Args:
            metric_data (dict): dictionary-representation of a given
                metric.

        Returns:
            dispatcher.BaseMetricDispatcher: a dispatcher relevant to
            metric type.
        """
        metric_type = metric_data.pop("type", None)
        name = metric_data.pop("name", None)
        value = metric_data.pop("value", None)
        kwargs = metric_data

        metric_method = {
            "counter": self.counter,
            "gauge": self.gauge,
            "timer": self.timer,
        }.get(metric_type)

        if not metric_method:
            msg = "Metric type '{}' not supported. Defaulting to gauge.".format(
                metric_type
            )
            logging.getLogger("klio.metrics").warning(msg)
            metric_method = self.gauge

        return metric_method(name=name, value=value, **kwargs)

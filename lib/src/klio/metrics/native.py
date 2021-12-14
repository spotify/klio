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
"""
Klio supports Apache Beam's `metrics
<https://beam.apache.org/documentation/programming-guide/#metrics>`_ via
:class:`NativeMetricsClient`.

When running on Dataflow, only Counter and Distribution type metrics are
emitted to Dataflow's monitoring. See `documentation
<https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring>`_
for more information.

This client is used by default regardless of runner, no configuration needed.
Configuration is supported for the unit of measure for timers objects in
``klio-job.yaml``:

.. code-block:: yaml

    job_config:
      metrics:
        native:
          # Default timer unit is s/seconds; available
          # options include `s` or `seconds`, `ms` or `milliseconds`,
          # `us` or `microseconds`, and `ns` or `nanoseconds`.
          timer_unit: ms

To configure a different unit of measure than the default for specific timers,
pass in the desired unit when instantiating. For example:

.. code-block:: python

    class MyTransform(KlioBaseTransform):
        def __init__(self):
            self.my_timer = self._klio.metrics.timer(
                name="my-timer", timer_unit="ns"
            )

.. caution::

    When running on Dataflow, in order for the Native metrics client to be able
    to report metrics to Stackdriver, the following ``experiment`` must be
    added to ``klio-job.yaml``:

    .. code-block:: yaml

        # <--snip-->
        pipeline_options:
          experiments:
            - enable_stackdriver_agent_metrics
        # <--snip-->
"""
import logging

from apache_beam import metrics as beam_metrics

from klio.metrics import base


# TODO: pull out into a general module since this is copied from logger.py
TIMER_UNIT_MAP = {
    "nanoseconds": "ns",
    "microseconds": "us",
    "milliseconds": "ms",
    "seconds": "s",
    "ns": "ns",
    "us": "us",
    "ms": "ms",
    "s": "s",
}
"""
Map of supported measurement units to shorthand for :class:`NativeTimer`.
"""


class NativeMetricsClient(base.AbstractRelayClient):
    """Metrics client for Beam-native metrics collection.

    Intended to be instantiated by :class:`klio.metrics.client.MetricsRegistry`
    and not by itself.

    Args:
        klio_config (klio_core.config.KlioConfig): the job's configuration.
    """

    RELAY_CLIENT_NAME = "beam"
    # Since these metrics show up on the right sidebar of the Dataflow UI,
    # let's default to seconds since the usual default of nanoseconds is
    # a bit unreadable (@lynn).
    DEFAULT_TIME_UNIT = "s"
    """Default unit of measurement for timer metrics."""

    def __init__(self, klio_config):
        super(NativeMetricsClient, self).__init__(klio_config)
        self.job_name = klio_config.job_name
        self.native_config = self.klio_config.job_config.metrics.get(
            "native", {}
        )
        self.timer_unit = self._set_timer_unit()

    def _set_timer_unit(self):
        timer_unit = NativeMetricsClient.DEFAULT_TIME_UNIT
        if isinstance(self.native_config, dict):
            _timer_unit = self.native_config.get("timer_unit")
            if _timer_unit:
                timer_unit = TIMER_UNIT_MAP.get(_timer_unit, timer_unit)
        return timer_unit

    def unmarshal(self, metric):
        # Method is not needed since we're not needing to unmarshal to/from
        # anywhere (unlike log-based metrics). The method still needs
        # implementation otherwise we'll get a TypeError when this class
        # gets instantiated.
        pass

    def emit(self, metric):
        # Beam handles the emitting to Dataflow monitoring already, so we
        # don't need to actually emit anything. The method still needs
        # implementation otherwise we'll get a TypeError when this class
        # gets instantiated.
        pass

    def counter(self, name, transform=None, **kwargs):
        """Create a :class:`NativeCounter` object.

        Args:
            name (str): name of counter
            transform (str): transform the counter is associated with. Defaults
                to the job's name.

        Returns:
            NativeCounter: a native counter instance
        """
        namespace = transform or self.job_name
        return NativeCounter(name, namespace=namespace)

    def gauge(self, name, transform=None, **kwargs):
        """Create a :class:`NativeGauge` object.

        Args:
            name (str): name of gauge
            transform (str): transform the gauge is associated with. Defaults
                to the job's name.

        Returns:
            NativeGauge: a native gauge instance
        """
        logger = logging.getLogger("klio.metrics")
        logger.warning(
            "Apache Beam's custom gauge-type metrics are not reported to "
            "Dataflow's Monitoring service. For more information: "
            "https://cloud.google.com/dataflow/docs/guides/"
            "using-cloud-monitoring"
        )

        namespace = transform or self.job_name
        return NativeGauge(name, namespace=namespace)

    def timer(self, name, transform=None, timer_unit=None, **kwargs):
        """Create a :class:`NativeTimer` object.

        Args:
            name (str): name of timer
            transform (str): transform the timer is associated with. Defaults
                to the job's name.
            timer_unit (str): timer unit; defaults to configured value
                in `klio-job.yaml`, or
                :attr:`NativeMetricsClient.DEFAULT_TIME_UNIT`. Options:
                :attr:`TIMER_UNIT_MAP`.

        Returns:
            NativeTimer: a native timer instance
        """
        namespace = transform or self.job_name
        if timer_unit:
            # Note: this should probably have better validation if it does
            # not recognize the unit given. Instead of erroring out, we'll
            # just use the default (@lynn)
            timer_unit = TIMER_UNIT_MAP.get(timer_unit, self.timer_unit)
        else:
            timer_unit = self.timer_unit
        return NativeTimer(name, namespace=namespace, timer_unit=timer_unit)


class NativeCounter(base.BaseMetric):
    """Counter metric using Beam's `counter-type metric
    <https://beam.apache.org/documentation/programming-guide/
    #types-of-metrics>`_.

    Args:
        name (str): name of counter
        namespace (str): Name of namespace the counter belongs to (e.g.
            the counter's transform).
    """

    def __init__(self, name, namespace):
        self._counter = beam_metrics.Metrics.counter(namespace, name)

    def update(self, value):
        self._counter.inc(value)


class NativeGauge(base.BaseMetric):
    """Gauge metric using Beam's `gauge-type metric
    <https://beam.apache.org/documentation/programming-guide/
    #types-of-metrics>`_.

    Args:
        name (str): name of gauge
        namespace (str): Name of namespace the gauge belongs to (e.g.
            the gauge's transform).
    """

    def __init__(self, name, namespace):
        self._gauge = beam_metrics.Metrics.gauge(namespace, name)

    def update(self, value):
        self._gauge.set(value)


class NativeTimer(base.BaseMetric):
    """Timer metric using Beam's `distribution-type metric
    <https://beam.apache.org/documentation/programming-guide/
    #types-of-metrics>`_.

    Args:
        name (str): name of timer
        namespace (str): Name of namespace the timer belongs to (e.g.
            the timer's transform).
        timer_unit (str): Unit of measurement. Options:
            :attr:`TIMER_UNIT_MAP`.
            Default: ``ns`` (nanoseconds).
    """

    def __init__(self, name, namespace, timer_unit="ns"):
        self._timer = beam_metrics.Metrics.distribution(namespace, name)
        self.timer_unit = timer_unit

    def update(self, value):
        # TODO: how is timer unit(s) handled?
        self._timer.update(value)

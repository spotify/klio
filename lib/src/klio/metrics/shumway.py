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
Klio supports emitting metrics for sending metrics to a `FFWD agent
<https://github.com/spotify/ffwd>`_ via the `shumway
<https://github.com/spotify/shumway>`_ library.

This client is enabled by default when running with Klio's DirectGKERunner.
This must be actively turned off in ``klio-job.yaml`` if not wanted:

.. code-block:: yaml

    job_config:
      metrics:
        shumway: False

Configuration is supported for the key used to report metrics, as well as unit
of measure for timers objects in ``klio-job.yaml``:

.. code-block:: yaml

    job_config:
      metrics:
        shumway:
          # The default value for `key` is the job's name.
          key: my-job-key
          # Default timer unit is ns/nanoseconds; available
          # options include `s` or `seconds`, `ms` or `milliseconds`,
          # `us` or `microseconds`, and `ns` or `nanoseconds`.
          timer_unit: seconds

To configure a different unit of measure than the default for specific timers,
pass in the desired unit when instantiating. For example:

.. code-block:: python

    class MyTransform(KlioBaseTransform):
        def __init__(self):
            self.my_timer = self._klio.metrics.timer(
                name="my-timer", timer_unit="ns"
            )
"""
import shumway

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
Map of supported measurement units to shorthand for :class:`ShumwayTimer`.
"""


class ShumwayMetricsClient(base.AbstractRelayClient):
    """Metrics client for `FFWD <https://github.com/spotify/ffwd>`_ metrics
    reporting via the `shumway <https://github.com/spotify/shumway>`_ library.

    Intended to be instantiated by :class:`klio.metrics.client.MetricsRegistry`
    and not by itself.

    Args:
        klio_config (klio_core.config.KlioConfig): the job's configuration.
    """

    RELAY_CLIENT_NAME = "shumway"
    DEFAULT_TIME_UNIT = "ns"
    DEFAULT_FFWD_ADDR = "127.0.0.1"
    """Default unit of measurement for timer metrics."""

    def __init__(self, klio_config):
        super(ShumwayMetricsClient, self).__init__(klio_config)
        self.job_name = klio_config.job_name
        self.shumway_config = self.klio_config.job_config.metrics.get(
            "shumway", {}
        )
        self.timer_unit = self._set_timer_unit()
        self._shumway_client = self._setup_client()

    def _set_timer_unit(self):
        timer_unit = ShumwayMetricsClient.DEFAULT_TIME_UNIT
        if isinstance(self.shumway_config, dict):
            _timer_unit = self.shumway_config.get("timer_unit")
            if _timer_unit:
                timer_unit = TIMER_UNIT_MAP.get(_timer_unit, timer_unit)
        return timer_unit

    def _setup_client(self):
        key = self.klio_config.job_name
        ffwd_addr = ShumwayMetricsClient.DEFAULT_FFWD_ADDR

        if isinstance(self.shumway_config, dict):
            key = self.shumway_config.get("key", key)
            ffwd_addr = self.shumway_config.get("ffwd_addr", ffwd_addr)

        return shumway.MetricRelay(key, ffwd_addr)

    def unmarshal(self, metric):
        """Return a dict-representation of a given metric.

        Args:
            metric (BaseShumwayMetric): logger-specific metrics object
        Returns:
            dict(str, str): metric data
        """
        metric.attributes["job_name"] = self.job_name
        return {
            "metric": metric.name,
            "value": metric.value,
            "attributes": metric.attributes,
        }

    def emit(self, metric):
        """Emit a metric to the FFWD agent.

        Args:
            metric (BaseShumwayMetric): logger-specific metrics object
        """
        metric_data = self.unmarshal(metric)
        self._shumway_client.emit(**metric_data)

    def counter(self, name, value=0, transform=None, tags=None, **kwargs):
        """Create a :class:`ShumwayCounter` object.

        Args:
            name (str): name of counter
            value (int): starting value of counter; defaults to 0
            transform (str): transform the counter is associated with
            tags (dict): any tags of additional contextual information
                to associate with the counter

        Returns:
            ShumwayCounter: a shumway-based counter
        """
        return ShumwayCounter(
            name, value=value, transform=transform, tags=tags, **kwargs
        )

    def gauge(self, name, value=0, transform=None, tags=None, **kwargs):
        """Create a :class:`ShumwayGauge` object.

        Args:
            name (str): name of gauge
            value (int): starting value of gauge; defaults to 0
            transform (str): transform the gauge is associated with
            tags (dict): any tags of additional contextual information
                to associate with the gauge

        Returns:
            ShumwayGauge: a shumway-based gauge
        """
        return ShumwayGauge(
            name, value=value, transform=transform, tags=tags, **kwargs
        )

    def timer(
        self,
        name,
        value=0,
        transform=None,
        timer_unit=None,
        tags=None,
        **kwargs
    ):
        """Create a :class:`ShumwayTimer` object.

        Args:
            name (str): name of timer
            value (int): starting value of timer; defaults to 0
            transform (str): transform the timer is associated with
            tags (dict): any tags of additional contextual information
                to associate with the timer
            timer_unit (str): timer unit; defaults to configured value
                in `klio-job.yaml`, or "ns". Options: :attr:`TIMER_UNIT_MAP`.

        Returns:
            ShumwayTimer: a shumway-based timer
        """
        if timer_unit:
            # Note: this should probably have better validation if it does
            # not recognize the unit given. Instead of erroring out, we'll
            # just use the default (@lynn)
            timer_unit = TIMER_UNIT_MAP.get(timer_unit, self.timer_unit)
        else:
            timer_unit = self.timer_unit
        return ShumwayTimer(
            name,
            value=value,
            transform=transform,
            timer_unit=timer_unit,
            tags=tags,
            **kwargs
        )


class BaseShumwayMetric(base.BaseMetric):
    """Base metric type for shumway.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with metric, if any.
        tags (dict): Tags to associate with metric.
    """

    def __init__(self, name, value=0, transform=None, tags=None, **kwargs):
        self.name = name
        self.value = value
        self.attributes = {"transform": transform}
        if tags is not None:
            assert isinstance(
                tags, dict
            ), "`tags` for metric objects should be dictionaries"
            # Note: if a user sets a `transform` key in `tags`, this will
            # override the above
            self.attributes.update(tags)


class ShumwayCounter(BaseShumwayMetric):
    """Shumway counter metric.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with counter, if any.
        tags (dict): Tags to associate with counter.
    """


class ShumwayGauge(BaseShumwayMetric):
    """Shumway gauge metric.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with counter, if any.
        tags (dict): Tags to associate with counter.
    """


class ShumwayTimer(BaseShumwayMetric):
    """Shumway timer metric.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with counter, if any.
        timer_unit (str): Unit of measurement. Options: :attr:`TIMER_UNIT_MAP`.
            Default: ``ns`` (nanoseconds).
        tags (list): Tags to associate with counter.
    """

    def __init__(
        self,
        name,
        value=0,
        transform=None,
        timer_unit="ns",
        tags=None,
        **kwargs
    ):
        super(ShumwayTimer, self).__init__(
            name, value=value, transform=transform, tags=tags, **kwargs
        )
        self.attributes["unit"] = timer_unit
        self.timer_unit = timer_unit

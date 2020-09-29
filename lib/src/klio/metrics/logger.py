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
"""
Klio ships with a default :class:`klio.metrics.base.AbstractRelayClient`
implementation, which outputs metrics via the standard library ``logging``
module through the :class:`MetricsLoggerClient` below.

This implementation is used by default if no other metrics consumers are
configured. It must be explicitly turned off.

The default configuration in ``klio-info.yaml`` can be overwritten:

.. code-block:: yaml

    job_config:
        metrics:
            logger:
                # Logged metrics are emitted at the `debug` level by default.
                level: info
                # Default timer unit is ns/nanoseconds; available
                # options include `s` or `seconds`, `ms` or `milliseconds`,
                # `us` or `microseconds`, and `ns` or `nanoseconds`.
                timer_unit: s

To turn off logging-based metrics:

.. code-block:: yaml

    job_config
        metrics:
            logger: false
"""

import logging
import threading

from klio.metrics import base


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
"""Map of supported measurement units to shorthand for :class:`LoggerTimer`.
"""


class MetricsLoggerClient(base.AbstractRelayClient):
    """Logging client for transform metrics.

    Intended to be instantiated by :class:`klio.metrics.client.MetricsRegistry`
    and not by itself.

    Args:
        klio_config (klio_core.config.KlioConfig):  the job's configuration.
        disabled (bool): whether or not to disable the Python ``logger``
            Default: ``False``.
    """

    RELAY_CLIENT_NAME = "logger"
    DEFAULT_LEVEL = logging.DEBUG
    DEFAULT_TIME_UNIT = "ns"

    _thread_local = threading.local()

    def __init__(self, klio_config, disabled=False):
        super(MetricsLoggerClient, self).__init__(klio_config)
        self.logger_config = self.klio_config.job_config.metrics.get(
            "logger", {}
        )
        self.disabled = disabled
        self.log_level = self._set_log_level()
        self.timer_unit = self._set_timer_unit()

    def _set_log_level(self):
        log_level = MetricsLoggerClient.DEFAULT_LEVEL
        if isinstance(self.logger_config, dict):
            log_level_str = self.logger_config.get("level")
            if log_level_str:
                log_level = getattr(logging, log_level_str.upper(), log_level)
        return log_level

    def _set_timer_unit(self):
        timer_unit = MetricsLoggerClient.DEFAULT_TIME_UNIT
        if isinstance(self.logger_config, dict):
            _timer_unit = self.logger_config.get("timer_unit")
            if _timer_unit:
                timer_unit = TIMER_UNIT_MAP.get(_timer_unit, timer_unit)
        return timer_unit

    @property
    def logger(self):
        """Python logger associated with the job which this client will use
        to emit metrics.
        """
        klio_metrics_logger = getattr(
            self._thread_local, "klio_metrics_logger", None
        )
        if not klio_metrics_logger:
            logger = logging.getLogger("klio.metrics")
            logger.disabled = self.disabled
            self._thread_local.klio_metrics_logger = logger
        return self._thread_local.klio_metrics_logger

    def unmarshal(self, metric):
        """Return a dict-representation of a given metric.

        Args:
            metric (LoggerMetric): logger-specific metrics object
        Returns:
            dict(str, str): metric data
        """
        return {
            "name": metric.name,
            "value": metric.value,
            "transform": metric.transform,
            "tags": metric.tags,
        }

    def emit(self, metric):
        """Log a given metric.

        Args:
            metric (LoggerMetric): logger-specific metrics object
        """
        metric_data = self.unmarshal(metric)
        self.logger.log(
            self.log_level, metric.DEFAULT_LOG_FORMAT.format(**metric_data)
        )

    def counter(self, name, value=0, transform=None, tags=None, **kwargs):
        """Create a :class:`LoggerCounter` object.

        Args:
            name (str): name of counter
            value (int): starting value of counter; defaults to 0
            transform (str): transform the counter is associated with
            tags (dict): any tags of additional contextual information
                to associate with the counter

        Returns:
            LoggerCounter: a log-based counter
        """
        return LoggerCounter(
            name=name, value=value, transform=transform, tags=tags
        )

    def gauge(self, name, value=0, transform=None, tags=None, **kwargs):
        """Create a :class:`LoggerGauge` object.

        Args:
            name (str): name of gauge
            value (int): starting value of gauge; defaults to 0
            transform (str): transform the gauge is associated with
            tags (dict): any tags of additional contextual information
                to associate with the gauge

        Returns:
            LoggerGauge: a log-based gauge
        """
        return LoggerGauge(
            name=name, value=value, transform=transform, tags=tags
        )

    def timer(
        self,
        name,
        value=0,
        transform=None,
        tags=None,
        timer_unit=None,
        **kwargs
    ):
        """Create a :class:`LoggerTimer` object.

        Args:
            name (str): name of timer
            value (int): starting value of timer; defaults to 0
            transform (str): transform the timer is associated with
            tags (dict): any tags of additional contextual information
                to associate with the timer
            timer_unit (str): timer unit; defaults to configured value
                in `klio-job.yaml`, or "ns". See module-level docs of
                `klio.metrics.logger` for supported values.

        Returns:
            LoggerTimer: a log-based timer
        """
        if timer_unit:
            # Note: this should probably have better validation if it does
            # not recognize the unit given. Instead of erroring out, we'll
            # just use the default (@lynn)
            timer_unit = TIMER_UNIT_MAP.get(timer_unit, self.timer_unit)
        else:
            timer_unit = self.timer_unit
        return LoggerTimer(
            name=name,
            value=value,
            transform=transform,
            tags=tags,
            timer_unit=timer_unit,
        )


class LoggerMetric(base.BaseMetric):
    """Base metric type for loggers.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with metric, if any.
        tags (dict): Tags to associate with metric.
    """

    LOGGER_METRIC_TAGS = None
    DEFAULT_LOG_FORMAT = (
        "[{name}] value: {value} transform: '{transform}' tags: {tags}"
    )

    def __init__(self, name, value=0, transform=None, tags=None):
        super(LoggerMetric, self).__init__(
            name, value=value, transform=transform
        )
        self.tags = tags if tags else {}
        self.tags.update(self.LOGGER_METRIC_TAGS)


class LoggerCounter(LoggerMetric):
    """Log-based counter metric.

    Args:
        name (str): name of counter
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with counter, if any.
        tags (dict): Tags to associate with counter. Note:
            ``{"metric_type": "counter"}`` will always be an included tag.
    """

    LOGGER_METRIC_TAGS = {"metric_type": "counter"}


class LoggerGauge(LoggerMetric):
    """Log-based gauge metric.

    Args:
        name (str): name of gauge
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with gauge, if any.
        tags (dict): Tags to associate with gauge. Note:
            ``{"metric_type": "gauge"}`` will always be an included tag.
    """

    LOGGER_METRIC_TAGS = {"metric_type": "gauge"}


class LoggerTimer(LoggerMetric):
    """Log-based timer metric.

    Args:
        name (str): name of timer
        value (int): initial value. Default: ``0``.
        transform (str): Name of transform associated with timer, if any.
        tags (dict): Tags to associate with timer. Note:
            ``{"metric_type": "timer"}`` will always be an included tag.
        timer_unit (str): Unit of measurement. Options: :attr:`TIMER_UNIT_MAP`.
            Default: ``ns`` (nanoseconds).
    """

    LOGGER_METRIC_TAGS = {"metric_type": "timer"}

    def __init__(
        self, name, value=0, transform=None, tags=None, timer_unit="ns"
    ):
        self.LOGGER_METRIC_TAGS.update({"unit": timer_unit})
        super(LoggerTimer, self).__init__(
            name, value=value, transform=transform, tags=tags
        )
        self.timer_unit = timer_unit

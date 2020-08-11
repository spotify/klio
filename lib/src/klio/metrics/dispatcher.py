# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB
"""Metric dispatchers for each metric type.

A dispatcher provides one single metric instance that will then interface
with all configured relay clients for that particular metric time. For
example:

    relay_clients = [
        klio.metrics.logger.MetricsLoggerClient(config),
        some_other_relay_client,
    ]
    my_counter = CounterDispatcher(relay_clients, name="my-counter")
    my_counter.inc()

Creating the `my_counter` instance will create two relay counter
instances, each specific to each of the relay clients configured.
Calling `inc()` on `my_counter` will then call `emit` on each relay
counter instance where each relay client will take care of its own
emit logic.
"""

import logging
import threading
import timeit

from concurrent import futures


# not making it an ABC because there shouldn't be a need to create custom
# dispatchers
class BaseMetricDispatcher(object):
    """Base class for metric-specific dispatching.

    Each type of metric (counter, gauge, timer) requires a dispatcher
    implementation.
    """

    METRIC_TYPE = None
    _thread_local = threading.local()

    def __init__(self, relay_clients, name, value=0, transform=None, **kwargs):
        self.name = name
        self.value = value
        self.transform = transform
        self.metric_key = self._setup_metric_key()
        self.kwargs = kwargs
        self.relay_to_metric = self._setup_metric_relay(relay_clients)
        self._thread_pool = futures.ThreadPoolExecutor(
            max_workers=len(relay_clients)
        )

    def _setup_metric_relay(self, relay_clients):
        raise NotImplementedError()

    def _setup_metric_key(self):
        metric_key = "{}_{}".format(self.METRIC_TYPE, self.name)
        if self.transform:
            metric_key = "{}_{}".format(metric_key, self.transform)
        return metric_key

    @property
    def logger(self):
        klio_metrics_dispatcher_logger = getattr(
            self._thread_local, "klio_metrics_dispatcher_logger", None
        )
        if not klio_metrics_dispatcher_logger:
            logger = logging.getLogger("klio.metrics.dispatcher")
            self._thread_local.klio_metrics_dispatcher_logger = logger
        return self._thread_local.klio_metrics_dispatcher_logger

    def _submit_callback(self, fut):
        try:
            fut.result()
            # no need to do anything if successful
        except Exception as e:
            msg = "Error emitting metric '{}': {}".format(fut.metric_key, e)
            self.logger.warning(msg)

    def submit(self, emit, metric):
        fut = self._thread_pool.submit(emit, metric)

        fut.metric_key = self.metric_key  # for easy identifying
        fut.add_done_callback(self._submit_callback)


class CounterDispatcher(BaseMetricDispatcher):
    """Counter-like object that will emit via all configured clients."""

    METRIC_TYPE = "counter"

    def _setup_metric_relay(self, relay_clients):
        return [
            (
                r,
                r.counter(
                    name=self.name,
                    value=self.value,
                    transform=self.transform,
                    **self.kwargs
                ),
            )
            for r in relay_clients
        ]

    def inc(self, value=1):
        """Increment counter.

        Calling this method will emit the metric via configured clients.

        Args:
            value (int): value with which to increment the counter;
                default is 1.
        """
        self.value += value

        for relay, counter in self.relay_to_metric:
            counter.update(self.value)
            self.submit(relay.emit, counter)


class GaugeDispatcher(BaseMetricDispatcher):
    """Gauge-like object that will emit via all configured clients."""

    METRIC_TYPE = "gauge"

    def _setup_metric_relay(self, relay_clients):
        return [
            (
                r,
                r.gauge(
                    name=self.name,
                    value=self.value,
                    transform=self.transform,
                    **self.kwargs
                ),
            )
            for r in relay_clients
        ]

    def set(self, value):
        """Set gauge to a given value.

        Calling this method will emit the metric via configured clients.

        Args:
            value (int): value with which to set the gauge.
        """
        self.value = value

        for relay, gauge in self.relay_to_metric:
            gauge.update(self.value)
            self.submit(relay.emit, gauge)


class TimerDispatcher(BaseMetricDispatcher):
    """Timer-like object that will emit via all configured clients.

    This may be used by instantiating and manually calling start & stop:

        timer = TimerDispatcher(relay_clients, name)
        timer.start()
        # code to time
        timer.stop()

    Or as a context manager:

        with TimerDispatcher(relay_clients, name):
            # code to time
    """

    METRIC_TYPE = "timer"
    TIMER_UNIT_TO_NUMBER = {
        "ns": 1e9,  # seconds -> nanoseconds, default
        "us": 1e6,  # seconds -> microseconds
        "ms": 1e3,  # seconds -> milliseconds
        "s": 1,
    }

    def __init__(
        self,
        relay_clients,
        name,
        value=0,
        transform=None,
        timer_unit="ns",
        **kwargs
    ):
        super(TimerDispatcher, self).__init__(
            relay_clients=relay_clients,
            name=name,
            value=value,
            transform=transform,
            timer_unit=timer_unit,
            **kwargs
        )
        self.timer_unit = timer_unit
        self._start_time = None

    def _setup_metric_relay(self, relay_clients):
        return [
            (
                r,
                r.timer(
                    name=self.name,
                    value=self.value,
                    transform=self.transform,
                    **self.kwargs
                ),
            )
            for r in relay_clients
        ]

    def start(self):
        """Start the timer.

        Note: this uses `timeit.default_timer`, which is `time.perf_counter`
        for Python 3.3 or later, and `time.time` for earlier versions of
        Python.
        """
        self._start_time = timeit.default_timer()

    def stop(self):
        """Stop the timer.

        Calling this method will emit the metric via configured clients.
        """
        if self._start_time is None:
            self.logger.warning(
                "Timer {} cannot be stopped before started.".format(
                    self.metric_key
                )
            )
            return

        time_elapsed = timeit.default_timer() - self._start_time
        self.value = time_elapsed * TimerDispatcher.TIMER_UNIT_TO_NUMBER.get(
            self.timer_unit, 1e9
        )

        for relay, timer in self.relay_to_metric:
            timer.update(self.value)
            self.submit(relay.emit, timer)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()

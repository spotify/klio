``klio.metrics`` Subpackage
===========================

.. toctree::
   :maxdepth: 1
   :hidden:

   Client <client>
   Native <native>
   Logger <logger>
   Shumway <shumway>
   Dispatcher <dispatcher>
   Base Classes <base>

.. automodule:: klio.metrics


:doc:`client`
^^^^^^^^^^^^^

.. currentmodule:: klio.metrics.client

.. autosummary::
    :nosignatures:

    MetricsRegistry
    MetricsRegistry.counter
    MetricsRegistry.gauge
    MetricsRegistry.timer
    MetricsRegistry.marshal
    MetricsRegistry.unmarshal


:doc:`native`
^^^^^^^^^^^^^

.. currentmodule:: klio.metrics.native

.. autosummary::
    :nosignatures:

    NativeMetricsClient
    NativeMetricsClient.counter
    NativeMetricsClient.gauge
    NativeMetricsClient.timer
    NativeCounter
    NativeGauge
    NativeTimer

:doc:`logger`
^^^^^^^^^^^^^

.. currentmodule:: klio.metrics.logger

.. autosummary::
    :nosignatures:

    MetricsLoggerClient
    MetricsLoggerClient.logger
    MetricsLoggerClient.unmarshal
    MetricsLoggerClient.emit
    MetricsLoggerClient.counter
    MetricsLoggerClient.gauge
    MetricsLoggerClient.timer
    LoggerMetric
    LoggerCounter
    LoggerGauge
    LoggerTimer


:doc:`shumway`
^^^^^^^^^^^^^^

.. currentmodule:: klio.metrics.shumway

.. autosummary::
    :nosignatures:

    ShumwayMetricsClient
    ShumwayMetricsClient.unmarshal
    ShumwayMetricsClient.emit
    ShumwayMetricsClient.counter
    ShumwayMetricsClient.gauge
    ShumwayMetricsClient.timer
    BaseShumwayMetric
    ShumwayCounter
    ShumwayGauge
    ShumwayTimer


:doc:`dispatcher`
^^^^^^^^^^^^^^^^^

.. currentmodule:: klio.metrics.dispatcher

.. autosummary::
    :nosignatures:

    BaseMetricDispatcher
    BaseMetricDispatcher.logger
    BaseMetricDispatcher.submit
    CounterDispatcher.inc
    GaugeDispatcher.set
    TimerDispatcher.start
    TimerDispatcher.stop


:doc:`base`
^^^^^^^^^^^

.. currentmodule:: klio.metrics.base

.. autosummary::
    :nosignatures:

    AbstractRelayClient
    AbstractRelayClient.unmarshal
    AbstractRelayClient.emit
    AbstractRelayClient.counter
    AbstractRelayClient.gauge
    AbstractRelayClient.timer
    BaseMetric
    abstract_attr

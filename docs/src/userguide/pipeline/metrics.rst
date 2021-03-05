.. _metrics:

Metrics
=======

Default Metrics Provided
------------------------

Coming soon!

Custom User Metrics
-------------------

Within a Klio transform, you are able to create metric objects during pipeline execution.
Klio defaults to using `Apache Beam's metrics <https://beam.apache.org/documentation/programming-guide/#metrics>`_ 
(referred to as "native" metrics within Klio), and additionally provides a metrics logger 
(via the standard library's :mod:`logging` module), and `Stackdriver log-based metrics 
<https://cloud.google.com/logging/docs/logs-based-metrics/>`_.
The Stackdriver log-based metrics client is an overlay of the metrics logger
where it creates a `user-defined metric <https://console.cloud.google.com/logs/metrics>`_
within Stackdriver per Python metric object created.
It is otherwise the same in terms of logging an event for measurement.

.. Warning::

    Please familiarize yourself with the limitations detailed :ref:`below <limitations>`.

Quickstart Example
------------------

.. code-block:: python

    import apache_beam as beam

    from klio.transforms import decorators

    class LogKlioMessage(beam.DoFn):
        @decorators.set_klio_context
        def __init__(self):
            self.model = None

            # a simple counter
            self.entity_ctr = self._klio.metrics.counter("entity-counter")

            # a counter specific to this transform
            self.success_ctr = self._klio.metrics.counter(
                "success-counter", transform=self.__class__.__name__
            )
            # a counter with user-defined tags
            self.error_ctr = self._klio.metrics.counter(
                "error-counter", tags={"job-version": "v1"}
            )

            # a gauge for a specific transform with tags
            self.model_memory_gauge = self._klio.metrics.gauge(
                "model-memory-gauge",
                transform=self.__class__.__name__,
                tags={"units": "mb"},
            )

            # a simple timer (defaults to nanoseconds)
            self.process_latency = self._klio.metrics.timer("process-latency")

            # a timer specific to transform with tags, using milliseconds
            self.load_model_latency = self._klio.metrics.timer(
                "load-model-latency",
                transform=self.__class__.__name__,
                tags={"units": "ms", "model_version": "2019-01-01"},
            )

        @decorators.set_klio_context
        def setup(self):
            # use timer as a context manager
            with self.load_model_latency:
                self.model = load("my-model.pb")

            # some way get the memory footprint
            model_memory = self.model.get_memory_usage()
            self.model_memory_gauge.set(model_memory)

        @decorators.handle_klio
        def process(self, item):
            self.entity_ctr.inc()  # increment counter
            self.process_latency.start()  # start timer directly

            try:
            # do the thing
                self._klio.logger.info("Hello, Klio!")
                self._klio.logger.info("Received element {}".format(item.element))
                self._klio.logger.info("Received payload {}".format(item.payload))

            except SomeException as e:
                self.error_ctr.inc()  # increment counter
                # do another thing
            else:
                self.success_ctr.inc()  # increment counter

            # stop counter directly, before yield'ing
            self.process_latency.stop()

            yield item


.. tip::

    Metrics objects should be
    created in the ``__init__`` method or the ``setup`` method of your transform.


Stackdriver Required Setup
--------------------------

Access Control
**************

Your default service account for the project must have at least
`Logs Configuration Writer
<https://cloud.google.com/logging/docs/access-control#permissions_and_roles>`_
permission in order to create metrics based off of logs.

Create Dashboard
****************

During the runtime of a pipeline, Klio will automatically create or reuse the
`user-defined metrics <https://console.cloud.google.com/logs/metrics>`_ in Stackdriver Logging.
Klio is not yet able to programmatically create dashboards in Stackdriver Monitoring,
but this functionality is coming soon!

Follow the
`Stackdriver documentation
<https://cloud.google.com/logging/docs/logs-based-metrics/charts-and-alerts>`_
on creating dashboards & charts for log-based metrics.


Configuration
-------------

With no additional configuration needed, metrics will be turned on and collected.

The default client depends on the runner:

| **Dataflow:** Stackdriver Log-based Metric Client
| **Direct:** Standard Library Log Metric Client

Default Configuration
*********************

In your ``klio-job.yaml``, if you accept the default configuration, you do not need to add anything.

Setting no metrics configuration is the same as:

.. code-block:: yaml

  job_config:
    metrics:
      native:
        # default timer unit in seconds
        timer_unit: s
      logger:  # default on for Direct Runner
        # level that metrics are emitted
        level: debug
        # default timer unit in nanoseconds
        timer_unit: ns
      stackdriver_logger:  # default on for Dataflow
        # level that metrics are emitted
        level: debug
        # default timer unit in nanoseconds
        timer_unit: ns

The default configuration above is the same as setting metrics clients to `True`:

.. code-block:: yaml

  job_config:
    metrics:
      logger: true
      stackdriver_logger: true


.. note::

    The native client can not be turned off.


To turn off/on a metrics client, set its value to `false`/`true`:

.. code-block:: yaml

  job_config:
    metrics:
      stackdriver_logger: false

.. note::

    While on Dataflow, setting ``logger`` to ``False``
    will have **no effect** when ``stackdriver_logger`` is still turned on.

.. note::

    While using the Direct runner, turning on ``stackdriver_logger``
    will have **no effect**.

    This is because Stackdriver log-based metrics requires logs to be sent to Stackdriver
    while the Direct runner sends logs to ``stdout``/``stderr``.


Available Configuration
***********************

For all three clients, ``native``, ``logger`` and ``stackdriver_logger``, the following configuration is available:


.. program:: metrics-config

.. option:: timer_unit

  Globally set the default unit of time for timers.

  Options: ``ns``, ``nanoseconds``, ``us``, ``microseconds``, ``ms``, ``milliseconds``,
  ``s``, ``seconds``.

  Default: ``ns``


For both ``logger`` and ``stackdriver_logger``, the following additional configuration is available:

.. program:: metrics-config

.. option:: level

  Level at which metrics are emitted.

  Options: ``debug``, ``info``, ``warning``, ``error``, ``critical``.

  Default: ``debug``


Metric Types
------------

Klio follows Dropwizard's `metric types <https://metrics.dropwizard.io/3.1.0/manual/core>`_ ,
in line with `heroic services <https://github.com/spotify/heroic>`_
and `Scio pipelines <https://github.com/spotify/scio>`_.

When creating/instantiated metric objects, a ``name`` argument is required.
Optional supported keyword arguments are ``transform=STR`` and ``tags=DICT``.
Every metric will have a tag key/value pair for ``metric_type``.

.. note::

    Metrics objects should be created in the ``__init__`` method
    or the ``setup`` method of your transform.

Counters
********

A simple integer that can only be incremented.

Usage examples:

.. code-block:: python

  # a simple counter
  my_counter = self._klio.metrics.counter("my-counter")

  # a counter specific to a transform
  my_counter = self._klio.metrics.counter(
    "my-counter", transform=self.__class__.__name__
  )
  my_counter = self._klio.metrics.counter(
    "my-counter", transform="MyTransform"
  )

  # a counter with user-defined tags
  my_counter = self._klio.metrics.counter(
    "my-counter",
    tags={"model-version": "v1", "image-version": "v1beta1"},
  )

  # incrementing a counter
  my_counter.inc()


How it looks:

.. code-block::

  INFO:klio:Got entity id: d34db33f
  INFO:klio.metrics:[my-counter] value: 1 transform:'MyTransform' tags: {'model-version': 'v1', 'image-version': 'v1beta1', 'metric_type': 'counter'}

Gauges
******

.. warning::

    At the moment, the Stackdriver log-based metrics client within Klio
    can **only** support counter-type metrics.
    You may still create gauge-type & timer-type metrics,
    but those will only show up in logs, not on Stackdriver.


.. warning::

    With the native Beam metrics, when running on Dataflow, only Counter and Distribution type metrics are emitted to Dataflow's monitoring. 
    See `documentation <https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring>`_ for more information.


A simple integer that is set.
It reflects a measurement at that point in time
(i.e. memory usage, number of currently-open connections).

Usage examples:

.. code-block:: python

  # a simple gauge
  my_gauge = self._klio.metrics.gauge("my-gauge")

  # a gauge specific to a transform
  my_gauge = self._klio.metrics.gauge(
    "my-gauge", transform=self.__class__.__name__
  )
  my_gauge = self._klio.metrics.gauge(
    "my-gauge", transform="MyTransform"
  )

  # a gauge with user-defined tags
  my_gauge = self._klio.metrics.gauge(
    "my-gauge",
    tags={
      "model-version": "v1",
      "image-version": "v1beta1",
      "units": "some-unit",
    },
  )

  # set a gauge
  my_gauge.set(42)


How it looks:

.. code-block::

  INFO:klio.metrics:[my-gauge] value: 42 transform: 'MyTransform' tags: {'units': 'some-unit', 'metric_type': 'gauge'}

Timers
******

.. warning::

    At the moment,
    the Stackdriver log-based metrics client within Klio can **only** support counter-type metrics.
    You may still create gauge-type & timer-type metrics,
    but those will only show up in logs, not on Stackdriver.

An integer reflected a duration of an event (i.e. time to process an entity, response latency).

You can measure duration with a timer object in two ways:
via the `start`/`stop` methods, or as a context manager (see examples below).

.. note::

    Timers default to measuring in nanoseconds (`ns`),
    but can be configured to measure in seconds (`s`), milliseconds (`ms`), or microseconds (`us`).

    This can be done within timer object creation, (example below),
    or globally via configuration (see [available configuration](#available-configuration)).
    Setting the unit on a specific timer will override the global configuration.

Usage Examples:

.. code-block:: python

    # a simple timer
    my_timer = self._klio.metrics.timer("my-timer")

    # a timer using seconds
    my_timer = self._klio.metrics.timer("my-timer", timer_unit="s")

    # a timer specific to a transform
    my_timer = self._klio.metrics.timer(
      "my-timer", transform=self.__class__.__name__
    )
    my_timer = self._klio.metrics.timer(
      "my-timer", transform="MyTransform"
    )

    # a timer with user-defined tags
    my_timer = self._klio.metrics.timer(
      "my-timer",
      tags={
        "model-version": "v1",
        "image-version": "v1beta1",
      },
    )

    # either start & stop a timer directly
    my_timer.start()
    # do the thing
    my_timer.stop()

    # or use it as a context manager
    with my_timer:
      # do things

How it looks:

.. code-block::

  INFO:klio.metrics:[my-timer] value: 562200.0026050955 transform: 'HelloKlio' tags: {'metric_type': 'timer', 'unit': 'ns'}


Unsupported Types
*****************

Unlike Scio pipelines and backend services,
Klio **cannot** support certain metric types, like histogram, meter, and deriving meter
due to :ref:`technical limitations <limitations>` imposed by Dataflow.
We will reinvestigate if/when those limitations are addressed.


.. _limitations:

Limitations
-----------

**Gauge & timer support in Stackdriver:**
Klio does not yet support gauges or timers for log-based metrics in Stackdriver
(they will still be logged to Stackdriver Logging, though).
Right now, Klio only relies on
`Stackdriver's construct of counters
<https://cloud.google.com/logging/docs/logs-based-metrics/#counter-metric>`_.
In the future, Klio may support gauges and/or timers through
`distribution-type metrics
<https://cloud.google.com/logging/docs/logs-based-metrics/#distribution_metrics>`_.
Users are free to experiment with creating distribution metrics by hand based off the logs.

**Metrics between transforms:**
Because Dataflow does not yet support stateful processing
for streaming Python pipelines (planned 2020),
maintaining metrics between transforms of a pipeline can not be supported
(i.e. timing an entity across a whole pipeline of multiple transforms.

**Stackdriver metrics for historical logs:**
In Stackdriver, metrics based off of logs will be tracked *after* the metric is created.
Stackdriver **will ignore** any previous log lines before the metric is made.

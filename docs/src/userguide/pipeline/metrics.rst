.. _metrics:

Metrics
=======

Default Metrics Provided
------------------------

Klio collects :ref:`some metrics <default-metrics-collected>` by default. 
When running on Dataflow, these metrics will be :ref:`automatically available <dataflow-metrics-ui>` in the job's UI as custom counters, as well as custom metrics in Stackdriver monitoring.

.. _default-metrics-collected:

Metrics Collected
*****************

.. _io-transform-metrics:

IO Transforms
~~~~~~~~~~~~~

The following metrics are collected by default from Klio's :doc:`IO transforms </reference/lib/api/transforms/io>`:

.. list-table::
    :widths: 20 10 40 30
    :header-rows: 1

    * - Name
      - Type
      - Description
      - Collecting Transform(s) 
    * - ``kmsg-read`` [#f0]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` read in as event input.
      - | :class:`KlioReadFromPubSub <klio.transforms.io.KlioReadFromPubSub>`
        | :class:`KlioReadFromBigQuery <klio.transforms.io.KlioReadFromBigQuery>`
        | :class:`KlioReadFromAvro <klio.transforms.io.KlioReadFromAvro>`
        | :class:`KlioReadFromText <klio.transforms.io.KlioReadFromText>`

    * - ``kmsg-write`` [#f0]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` written out as event output.
      - | :class:`KlioWriteToPubSub <klio.transforms.io.KlioWriteToPubSub>`
        | :class:`KlioWriteToBigQuery <klio.transforms.io.KlioWriteToBigQuery>`
        | :class:`KlioWriteToAvro <klio.transforms.io.KlioWriteToAvro>`
        | :class:`KlioWriteToText <klio.transforms.io.KlioWriteToText>`


.. [#f0] Collecting transform is automatically used in pipeline unless configured otherwise. See :ref:`builtin-transforms` for more information on what is built-in within a Klio pipeline.


Helper Transforms
~~~~~~~~~~~~~~~~~
        
The following metrics are collected by default from Klio's :doc:`helper transforms </reference/lib/api/transforms/helpers>`:

.. list-table::
    :widths: 20 10 40 30
    :header-rows: 1

    * - Name
      - Type
      - Description
      - Collecting Decorator(s) 
    * - ``kmsg-data-found-input`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` with input data found.
      - :class:`KlioGcsCheckInputExists <klio.transforms.helpers.KlioGcsCheckInputExists>`
    * - ``kmsg-data-not-found-input`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` with input data **not** found.
      - :class:`KlioGcsCheckInputExists <klio.transforms.helpers.KlioGcsCheckInputExists>`
    * - ``kmsg-data-found-output`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` with output data found.
      - :class:`KlioGcsCheckOutputExists <klio.transforms.helpers.KlioGcsCheckOutputExists>`
    * - ``kmsg-data-not-found-output`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` with output data **not** found.
      - :class:`KlioGcsCheckOutputExists <klio.transforms.helpers.KlioGcsCheckOutputExists>`
    * - ``kmsg-process-ping`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` **not** in ping mode and will be processed.
      - :class:`KlioFilterPing <klio.transforms.helpers.KlioFilterPing>`
    * - ``kmsg-skip-ping`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` in ping mode, and will skip processing to be passed through directly to the configured event output, if any. 
      - :class:`KlioFilterPing <klio.transforms.helpers.KlioFilterPing>`
    * - ``kmsg-process-force`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` in force mode, and will be processed even though its referenced output data exists.
      - :class:`KlioFilterForce <klio.transforms.helpers.KlioFilterForce>`
    * - ``kmsg-skip-force`` [#f1]_
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` **not** in force mode, and will skip processing because its referenced output data already exists. 
      - :class:`KlioFilterForce <klio.transforms.helpers.KlioFilterForce>`
    * - ``kmsg-output``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` written to event output specifically through the ``KlioWriteToEventOutput`` transform (excludes the final event output automatically handled by Klio with :ref:`kmsg-write <io-transform-metrics>`).
      - :class:`KlioWriteToEventOutput <klio.transforms.helpers.KlioWriteToEventOutput>`
    * - ``kmsg-drop``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` processed by the ``KlioDrop`` transform and will be dropped.
      - :class:`KlioDrop <klio.transforms.helpers.KlioDrop>`
    * - ``kmsg-drop-not-recipient``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` dropped because the message is not intended for the current job to handle.
      - :class:`KlioCheckRecipients <klio.transforms.helpers.KlioCheckRecipients>`  
    * - ``kmsg-debug``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` processed by the ``KlioDebugMessage`` transform.
      - :class:`KlioDebugMessage <klio.transforms.helpers.KlioDebugMessage>`  
    * - ``kmsg-trigger-upstream``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` emitted to upstream's event input via ``KlioTriggerUpstream``.
      - :class:`KlioTriggerUpstream <klio.transforms.helpers.KlioTriggerUpstream>`  



.. [#f1] Collecting transform is automatically used in pipeline unless configured otherwise. See :ref:`builtin-transforms` for more information on what is built-in within a Klio pipeline.

.. _default-decorator-metrics:

Decorators
~~~~~~~~~~

Klio also collects transform-level metrics through many of the built-in :doc:`decorators </reference/lib/api/transforms/decorators>`.

.. note:: 

    These metrics are on the transform-level, not pipeline level. 
    Therefore, each metric name plus the associated transform will count as one unique metric.

    For example, if there are two transforms that use the ``@handle_klio`` decorator, then **two** sets of metrics (i.e. ``kmsg-received``, ``kmsg-success``, ``kmsg-drop-error``, ``kmsg-timer``) will be collected, **one per transform**.

The following metrics are collected by default:
      
.. list-table::
    :widths: 20 10 40 30
    :header-rows: 1

    * - Name
      - Type
      - Description
      - Collecting Transform(s) 
    * - ``kmsg-received``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` received by a transform (before processing begins).
      - | :func:`@handle_klio <klio.transforms.decorators.handle_klio>`
        | :func:`@serialize_klio_message <klio.transforms.decorators.serialize_klio_message>`
    * - ``kmsg-success``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` successfully processed by a transform.
      - | :func:`@handle_klio <klio.transforms.decorators.handle_klio>`
        | :func:`@serialize_klio_message <klio.transforms.decorators.serialize_klio_message>`
    * - ``kmsg-drop-error``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` dropped because of error during processing. This includes messages dropped from retries exhausted (``kmsg-drop-retry-error``) and messages timing out (``kmsg-drop-timed-out``). This does **not** include messages dropped via ``KlioDrop`` transform (``kmsg-drop``).
      - | :func:`@handle_klio <klio.transforms.decorators.handle_klio>`
        | :func:`@serialize_klio_message <klio.transforms.decorators.serialize_klio_message>`
    * - ``kmsg-timer``
      - :class:`timer <klio.metrics.dispatcher.TimerDispatcher>`
      - Time it takes to process ``KlioMessage``. This includes messages that are processed successfully as well as messages that have been dropped because of error.

        This timer defaults to measuring in units as configured in ``klio-job.yaml`` under |job_config.metrics|_ in the following order of precedence:
            
        1. ``.timer_unit``
        2. ``.stackdriver_logger.timer_unit``
        3. ``.logger.timer_unit``
        4. If nothing is set, then ``seconds`` will be used.

      - | :func:`@handle_klio <klio.transforms.decorators.handle_klio>`
        | :func:`@serialize_klio_message <klio.transforms.decorators.serialize_klio_message>`
    * - ``kmsg-retry-attempt``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - Number of retries for a given ``KlioMessage``.
      - :func:`@retry <klio.transforms.decorators.retry>`
    * - ``kmsg-drop-retry-error``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - ``KlioMessage`` dropped from exhausting the number of configured retries. This number is included in ``kmsg-drop-error``.
      - :func:`@retry <klio.transforms.decorators.retry>`
    * - ``kmsg-drop-timed-out``
      - :class:`counter <klio.metrics.dispatcher.CounterDispatcher>`
      - Processing timed out for a ``KlioMessage``.  This number is included in ``kmsg-drop-error``.
      - :func:`@timeout <klio.transforms.decorators.timeout>`

.. _dataflow-metrics-ui:

Viewing Emitted Metrics
***********************

When using Dataflow, metrics will be automatically emitted to `Dataflow & Stackdriver monitoring <https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring#custom_metrics>`_.


For example, in Dataflow's job UI, within the right-side column listing "Job Info", a "Custom Counters" section should be visible (and will include any :ref:`custom user metrics <custom-user-metrics>`):

.. figure:: images/dataflow_counters.png
    :alt: Metrics viewed under "Custom Counters" in Dataflow Job UI
    :align: center
    :scale: 30%

    *Metrics viewed under "Custom Counters" in Dataflow Job UI*


All metrics, both these default metrics as well as :ref:`custom user metrics <custom-user-metrics>`, are also available in `Stackdriver Monitoring <https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring#explore_metrics>`_.

For example, when creating a graph for a `dashboard <https://cloud.google.com/monitoring/charts>`_, select the resource type "Dataflow Job", and then the desired metric to graph under "Metric". 
Add a filter for a particular transform to avoid viewing a metric of the same name for all the transforms (particularly useful for metrics collected via :ref:`decorators <default-decorator-metrics>`).

.. figure:: images/dashboard_create.png
    :alt: Klio metrics available in Stackdriver Monitoring Dashboards
    :align: center
    :scale: 30%

    *Klio metrics available in Stackdriver Monitoring Dashboards*

Any :ref:`custom user metrics <custom-user-metrics>` defined in a job's transforms should also be available to select under "Metric", too.


.. _custom-user-metrics:

Custom User Metrics
-------------------

Within a Klio transform, you are able to create metric objects during pipeline execution.
Klio defaults to using `Apache Beam's metrics <https://beam.apache.org/documentation/programming-guide/#metrics>`_ 
(referred to as "native" metrics within Klio), and additionally provides a metrics logger 
(via the standard library's :mod:`logging` module).

.. _stackdriver-log-metrics-deprecation-notice:

.. admonition:: Deprecated: Stackdriver Log-based Metrics
    :class: warning

    Klio's support for `Stackdriver log-based metrics <https://cloud.google.com/logging/docs/logs-based-metrics/>`_ 
    has been deprecated since version ``21.3.0`` and will be removed in a future release.
    Instead, Klio now provides a :class:`NativeMetricsClient <klio.metrics.native.NativeMetricsClient>` 
    that will automatically create and emit metrics to Dataflow Monitoring when the job runs on 
    Dataflow via Beam's `metrics API <https://beam.apache.org/documentation/programming-guide/#metrics>`_.
    
    Klio uses this native metrics client automatically, so **no migration changes are needed**.

Quickstart Example
------------------

.. code-block:: python

    import apache_beam as beam

    from klio.transforms import decorators

    class LogKlioMessage(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
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
    created in the ``setup`` method of your transform.


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
      stackdriver_logger: 
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

    Metrics objects should be created in the ``setup`` method of your transform.


.. caution::

    Native metric objects do not support the ``tags`` argument due to limitations in the Beam 
    `metrics API <https://beam.apache.org/documentation/programming-guide/#metrics>`_.
    If given, ``tags`` will be ignored.

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


How it looks with the :class:`logger <klio.metrics.logger.MetricsLoggerClient>` client:

.. code-block::

  INFO:klio:Got entity id: d34db33f
  INFO:klio.metrics:[my-counter] value: 1 transform:'MyTransform' tags: {'model-version': 'v1', 'image-version': 'v1beta1', 'metric_type': 'counter'}


.. hint::

    The :class:`NativeMetricsClient <klio.metrics.native.NativeMetricsClient>` will not log anything.

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


How it looks with the :class:`logger <klio.metrics.logger.MetricsLoggerClient>` client:

.. code-block::

  INFO:klio.metrics:[my-gauge] value: 42 transform: 'MyTransform' tags: {'units': 'some-unit', 'metric_type': 'gauge'}

.. hint::

    The :class:`NativeMetricsClient <klio.metrics.native.NativeMetricsClient>` will not log anything.

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

How it looks with the :class:`logger <klio.metrics.logger.MetricsLoggerClient>` client:

.. code-block::

  INFO:klio.metrics:[my-timer] value: 562200.0026050955 transform: 'HelloKlio' tags: {'metric_type': 'timer', 'unit': 'ns'}

.. hint::

    The :class:`NativeMetricsClient <klio.metrics.native.NativeMetricsClient>` will not log anything.

Unsupported Types
*****************

Unlike Scio pipelines and backend services,
Klio **cannot** support certain metric types, like histogram, meter, and deriving meter
due to :ref:`technical limitations <limitations>` imposed by Dataflow.
We will reinvestigate if/when those limitations are addressed.


Stackdriver Required Setup
--------------------------

.. caution::

    Support for Stackdriver log-based metrics has been marked for deprecation.
    See :ref:`above <stackdriver-log-metrics-deprecation-notice>` for more information.

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
<https://cloud.google.com/logging/docs/logs-based-metrics/#distribution-metric>`_.
Users are free to experiment with creating distribution metrics by hand based off the logs.

**Metrics between transforms:**
Because Dataflow does not yet support stateful processing
for streaming Python pipelines (planned 2020),
maintaining metrics between transforms of a pipeline can not be supported
(i.e. timing an entity across a whole pipeline of multiple transforms.

**Stackdriver metrics for historical logs:**
In Stackdriver, metrics based off of logs will be tracked *after* the metric is created.
Stackdriver **will ignore** any previous log lines before the metric is made.

.. |job_config.metrics| replace:: ``job_config.metrics``
.. _job_config.metrics: #job-config-metrics
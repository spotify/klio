.. _kliocontext:

The Klio Context
================

The ``KlioContext`` is an object containing useful state about your job.

You can make it available to your transform using one of the :ref:`KlioContext decorators <klio-context-decorators>`.


``config``
----------
The job's configuration as a KlioConfig object, which provides the job configuration as a :ref:`KlioJobConfig <job-config>` object and pipeline options as :ref:`KlioPipelineConfig <kliopipelineconfig>` object .

.. code-block:: python

    import apache_beam as beam
    from klio.transforms import decorators
    # <!-- snip -->
    class MyTransform(beam.DoFn):
        # <-- snip -->
        @decorators.handle_klio
        def process(self, item):
            self._klio.logger.info("Project? %s" % self._klio.config.pipeline_options.project)
            self._klio.logger.info("Job Name? %s"% self._klio.config.job_config.job_name)
            # do actual stuff
            yield item


``job``
-------
Provides access to an instance of :ref:`kliojob`, representing the currently running job within your transform.

.. code-block:: python

    from klio.transforms import decorators

    @decorators.handle_klio
    def my_map_func(ctx, data):
        ctx.logger.info("Who am I? %s" % ctx._klio.job.job_name)

    @decorators.inject_klio_context
    def my_map_func(ctx, data):
        ctx.logger.info("Who am I? %s" % ctx._klio.job.job_name)


``logger``
----------

A namespaced logger that helps the user differentiate their transform logs from Apache Beam-related logs.

.. code-block:: python

    import apache_beam as beam
    from klio.transforms import decorators
    # <-- snip -->
    class MyTransform(beam.DoFn):
        # <-- snip -->
        @decorators.handle_klio
        def process(self, item):
            self._klio.logger.info("Now processing %s" % item.element)
            # do stuff
            yield item


.. note::

    The default log level is ``logging.WARNING``.
    To change that, you can set the default lower in ``run.py``.

    .. code-block:: python

        # jobs/my-job/run.py

        import logging

        logging.getLogger("klio").setLevel(logging.DEBUG)

        # <-- snip -->
        def run(input_pcol, config):
        # <-- snip -->


``metrics``
-----------

A metrics registry object for emitting :ref:`metrics <metrics>` on the current job.

.. code-block:: python

    import apache_beam as beam
    from klio.transforms import decorators

    class MyTransform(beam.DoFn):
        # <-- snip -->
        @decorators.set_klio_context
        def setup(self):
            # a counter with user-defined tags
            my_counter = self._klio.metrics.counter(
                "my-counter",
                tags={"model-version": "v1", "image-version": "v1beta1"},
            )

        @decorators.handle_klio
        def process(self, item):
            # incrementing a counter
            my_counter.inc()
            yield item

Job Configuration
=================

.. toctree::
   :maxdepth: 1
   :hidden:

   pipeline_options
   job_config
   event_config
   data_config

A Klio job's configuration is defined in ``klio-job.yaml``.

.. tip::

    Klio defaults to reading a YAML file named ``klio-job.yaml`` for configuration.

    Point the ``klio`` command to a different configuration filename via the ``-c/--config-file``
    flag, e.g.

    .. code:: console

        $ klio job run --config-file klio-job-staging.yaml


File Structure
--------------

A ``klio-job.yaml`` file have the following top-level keys:

.. option:: version INT

    | Version of Klio job.
    |
    | **Options**: ``1``, ``2``.
    | **Default**: ``2``.
    | *Optional*

    .. warning::

        Version ``1`` has been **deprecated and removed** from Klio as of ``klio-cli`` version
        ``1.0.0``, ``klio`` version ``0.2.0``, and ``klio-exec`` version ``0.2.0``.

.. option:: job_name STR

    The job’s name. This should be unique within a particular GCP project - different projects
    can have a job with the same name.

    *Required*


.. option:: pipeline_options DICT

    Configuration under ``pipeline_options`` map directly to Beam and Dataflow options. See
    :doc:`pipeline_options` for some commonly-used options, but any option that Beam & Dataflow
    support are also supported here. More information can be found in the `official Beam docs`_.

    *Required*


.. option:: job_config DICT

    User-specified custom and Klio-specific job configuration. See :doc:`job_config` for
    supported Klio configuration as well as where to specify any additional
    :ref:`custom configuration <custom-conf>` needed for a job.

    *Required*


Further Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. toctree::
   :maxdepth: 1

   pipeline_options
   job_config
   event_config
   data_config

.. _access-config:

Accessing Configuration in a Job
--------------------------------

A Klio job has access to its configuration in a couple of ways: via the job's pipeline definition,
and within the transforms themselves.



Pipeline
^^^^^^^^

In a Klio job's ``run.py`` file where the pipeline is defined, the ``run`` function will be
provided with the ``KlioContext.config`` object. For example:

.. code-block:: python

    # in run.py

    def run(in_pcol, config):
        my_custom_config = config.job_config.my_custom_config

        out_pcol = in_pcol | MyTransform(my_custom_config)

        return out_pcol


.. todo:: add link to ``KlioContext`` above once its documented

.. important::

    When using a runner other than ``DirectRunner``, if access to configuration is needed within a
    transform's logic, use the approached defined :ref:`below <transforms-config>`. This is because
    the ``config`` object itself is not pickle-able.

    Instantiating class-based transforms occur during the launching of the pipeline, then gets
    pickled to be unloaded onto the remote worker (e.g. a Dataflow worker). Therefore, any instance
    variable defined in a transform's ``__init__`` method that is **not a pickable object** will
    not be accessible to the rest of the class when the worker executes the transform's logic.


.. _transforms-config:

Transforms
^^^^^^^^^^

While the job's configuration is automatically provided to a job's ``run.py::run`` function, if
access to configuration is needed for a transform's logic, then use the provided
:doc:`../pipeline/utilities`.

.. code-block:: python

    # in transforms.py

    import apache_beam as beam
    from klio.transforms import decorators

    class MyTransform(beam.DoFn):
        @decorators.handle_klio
        def process(self, item):
            my_custom_config = self._klio.config.job_config.my_custom_config
            ...


Examples
--------

Streaming
^^^^^^^^^

*Case:*

  * **Runner**: DirectRunner
  * **Events**: Consume ``KlioMessage`` events from a Google Pub/Sub subscription; write ``KlioMessage`` events to a Google Pub/Sub topic.
  * **Data**: Read input binary data from a GCS bucket; write output binary data to a GCS bucket.


.. literalinclude:: examples/streaming.yaml
  :language: yaml


*Case:*

  * **Runner**: DataflowRunner
  * **Events**: Consume ``KlioMessage`` events from a Google Pub/Sub subscription; write ``KlioMessage`` events to a Google Pub/Sub topic.
  * **Data**: Read input binary data from a GCS bucket; write output binary data to a GCS bucket.


.. literalinclude:: examples/streaming-dataflow.yaml
  :language: yaml


Batch
^^^^^

*Case:*

  * **Runner**: DirectRunner
  * **Events**: Generate ``KlioMessage`` events from a local file; write ``KlioMessage`` events to a local file.
  * **Data**: Read input binary data from a GCS bucket; write output binary data to a GCS bucket.


.. literalinclude:: examples/batch.yaml
  :language: yaml

*Case:*

  * **Runner**: DataflowRunner
  * **Events**: Generate ``KlioMessage`` events from a local file; write ``KlioMessage`` events to a local file.
  * **Data**: Read input binary data from a GCS bucket; write output binary data to a GCS bucket.
  * Use `Python packaging for dependency management`_ instead of using/packaging with Docker to run on the workers

.. literalinclude:: examples/batch-dataflow.yaml
  :language: yaml



.. _official Beam docs: https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
.. _driver: https://beam.apache.org/documentation/programming-guide/#overview
.. _Python packaging for dependency management: https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/

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


Templating and Overriding a Job's Configuration
-----------------------------------------------

Klio supports templating ``klio-job.yaml`` as well as overriding the configuration's values via the CLI.

Templating Keys and Values
^^^^^^^^^^^^^^^^^^^^^^^^^^

To template any key or value in ``klio-job.yaml``, declare it with ``${...}``, for example:

.. code-block:: yaml

  # klio-job.yaml snippet
  job_name: my-job
  job_config:
    a_key: ${my_template_value}
    ${a_template_key}: some_static_value


Then, use the ``--template`` flag (supported on most ``klio job`` subcommands – see :doc:`CLI documentation </reference/cli/api/cli/job>` for which commands supports the flag) to provide the content for the templated variables:

.. code-block:: sh

  # command accepts multiple `--template` flags
  $ klio job run \
    --template my_template_value=foo \
    --template a_template_key=some_variable_key


Within your Klio job, you'll have access to these configuration values just like normal:

.. code-block:: python

    # in transforms.py

    import apache_beam as beam
    from klio.transforms import decorators

    class MyTransform(beam.DoFn):
        @decorators.handle_klio
        def process(self, item):
            my_templated_value = self._klio.config.job_config.a_key
            # my_templated_value == 'foo'
            ...


Overriding Values
^^^^^^^^^^^^^^^^^

Similar to templating, Klio supports overriding the values of keys during runtime.
While templating can be used for both keys and values in ``klio-job.yaml``, overriding is limited to just values.

For example, a snippet of a Klio job's configuration might look like:

.. code-block:: yaml

  # klio-job.yaml snippet
  job_name: my-job
  job_config:
    a_key: a_default_value
    b_key: another_default_value
    c_key: some_other_default_value


Then, use the ``--override`` flag (supported on most ``klio job`` subcommands – see :doc:`CLI documentation </reference/cli/api/cli/job>` for which commands supports the flag) to override the desired keys:

.. code-block:: sh

  # command accepts multiple `--override` flags
  $ klio job run \
    --override job_config.a_key=a_new_value \
    --override job_config.b_key=b_new_value

.. attention::

  Be sure to provide the full path to the desired key to change.
  In this example, ``a_key`` is nested under ``job_config``, so the full path is ``job_config.a_key``.

Within your Klio job, you'll have access to these configuration values just like normal:

.. code-block:: python

    # in transforms.py

    import apache_beam as beam
    from klio.transforms import decorators

    class MyTransform(beam.DoFn):
        @decorators.handle_klio
        def process(self, item):
            a_value = self._klio.config.job_config.a_key
            # a_value == 'a_new_value'
            c_value = self._klio.config.job_config.c_key
            # c_value == 'some_other_default_value'
            ...

.. attention::

  In order to override a value for a data/event input or output, you must specify a name in its configuration, and then use the name in the path of the override:

  .. code-block:: yaml

    job_config:
      events:
        inputs:
          - type: pubsub
            name: my_input
            topic: my/pubsub/topic
            subscription: my/pubsub/subscription

  .. code-block:: sh

    $ klio job run \
      --override job_config.events.inputs.my_input.topic=/my/other/pubsub/topic

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

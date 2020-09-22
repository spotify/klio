Transforms
==========

.. todo::

    write a section on implementing one's own transform

.. _helper-transforms:

Helper Transforms
-----------------

Helper transforms aid existence checks, ping mode, and force re-generating output.
These transforms can be imported from ``klio.transforms.helpers``.
Many are used by default in a Klio pipeline.

.. _builtin-transforms:

Built-in Transforms
^^^^^^^^^^^^^^^^^^^

Built-in transforms are used **by default** in a Klio pipeline, but can be turned off if needed.

.. todo::

    add prose and/or a diagram of what transforms are automatically done and in what order (similar to the message process logic but transform-specific; maybe a visual like dataflow-esque).

.. _data-existence-checks:

Data Existence Checks
~~~~~~~~~~~~~~~~~~~~~

For :ref:`data IO types <data-inputs-type>` of ``gcs``, Klio will perform the input
existence check for you. Data input and output existence checks are configured by the
:ref:`existence check <skip-input-ext-check>`. These two configuration field default to ``False``
and will therefore conduct transforms :class:`KlioGcsCheckInputExists
<klio.transforms.helpers.KlioGcsCheckInputExists>` and :class:`KlioGcsCheckOutputExists
<klio.transforms.helpers.KlioGcsCheckOutputExists>` automatically. If :ref:`custom data existence
checks <custom-existence-checks>` are preferred then these fields should be set to ``True``.


:class:`KlioGcsCheckInputExists<klio.transforms.helpers.KlioGcsCheckInputExists>` and
:class:`KlioGcsCheckOutputExists<klio.transforms.helpers.KlioGcsCheckOutputExists>` work by
inspecting data configuration for the fields for :ref:`data locations <data-inputs-location>`
``job_config.data.(in|out)puts[].location`` and :ref:`file suffix <data-inputs-file-suffix>`
``job_config.data.(in|out)puts[].file_suffix``. For example, if we have an element that represents
a track ID ``f00b4r``, Klio would inspect the existence of the path: ``gs://foo-proj-input/
example-streaming-parent-job-output/f00b4r.ogg``.


``KlioGcsCheckInputExists``
***************************

:class:`KlioGcsCheckInputExists<klio.transforms.helpers.KlioGcsCheckInputExists>` is a `Composite
Transform`_ to check the input data existence in GCS. The transform utilizes `Tagged Outputs`_ to
label output as either as ``not_found`` or ``found``.

.. code-block:: python

    class MyFilterInput(beam.PTransform):
        """Composite transform to filter input data"""

        def expand(self, pcoll):

            # Check if input data exists
            input_data = pcoll | "Input Exists Filter" >> KlioGcsCheckInputExists()

            # Do something with the data that does not exist
            _ = input_data.not_found | "Not Found Data Transform" >> MyTransform()

            # Do something with the data does exist
            return input_data.found

``KlioGcsCheckOutputExists``
****************************

:class:`KlioGcsCheckOutputExists<klio.transforms.helpers.KlioGcsCheckOutputExists>` is a `Composite
Transform`_ to check the output exists in GCS. The transform utilizes `Tagged Outputs`_ to label
output as either  ``not_found`` or ``found``.

.. code-block:: python

    class MyGcsFilterOutput(beam.PTransform):
        """Composite transform to filter output data."""

        def expand(self, pcoll):
            # Check if output data exists
            output_exists = pcoll | "Output Exists Filter" >> KlioGcsCheckOutputExists()

            # Do something with output data that is found
            to_filter = output_exists.found | "Transform Found Data" >> MyTransformAlreadyFound()

            # Do something with the output data that is not found
            to_process = output_exists.not_found | "Data Not Found" >> MyTransformNotFound()


Data Filtering
~~~~~~~~~~~~~~

``KlioFilterPing``
******************

:class:`KlioFilterPing <klio.transforms.helpers.KlioFilterPing>` is a `Composite Transform`_ to
tag outputs if in :ref:`ping mode <ping-mode>` or not. The transform utilizes `Tagged Outputs`_
to label output as either ``pass_thru`` or ``process``.


.. code-block:: python

    class MyGcsFilterToProcess(beam.PTransform):
        """Composite transform to filter PCollections for processing"""

        def expand(self, pcoll):
            ping_pcoll = pcoll | "Ping Filter" >> KlioFilterPing()

            # handle any items that should just be sent to output directly
            _ = ping_pcoll.pass_thru | "Passthru Ping" >> MyPassThruTransform()

            out_pcoll = ping_pcoll.process | "Process Data" >> MyPrcessTransform()

.. _filter-force:

``KlioFilterForce``
*******************

:class:`KlioFilterForce <klio.transforms.helpers.KlioFilterForce>` is a `Composite Transform`_ to
filter if existing output should be :ref:`force-processed <force-mode>`. The transform will look
at a job's configuration for whether or not there is a global (pipeline-wide) forcing of messages
with already-existing output. It will first inspect whether a message has an explicit ``True`` or
``False`` set for force processing. If force mode is not set, then ``KlioFilterForce`` will
inspect the pipeline configuration. The default is ``False``. The ``KlioFilterForce`` transform
uses utilizes `Tagged Outputs`_ to label output as either ``pass_thru`` or``process``.


.. code-block:: python

    class KlioGcsFilterOutput(beam.PTransform):
        """Klio composite transform to filter output data.
        """

        def expand(self, pcoll):
            # Check if output data exists
            output_exists = pcoll | "Output Exists Filter" >> KlioGcsCheckOutputExists()

            # Filter if existing output should be force-processed
            output_force = output_exists.found | "Force Filter" >> KlioFilterForce()

            # handle any items that should just be sent to output directly
            _ = output_force.pass_thru | "Passthru Found Output" >> KlioWriteToEventOutput()

            # Handle items that should be force processed
            to_process = (output_exists.not_found, output_force.process)


``KlioCheckRecipients``
***********************

.. todo::

    fill me in

Other Built-in Transforms
~~~~~~~~~~~~~~~~~~~~~~~~~

``KlioUpdateAuditLog``
**********************

:class:`KlioUpdateAuditLog <klio.transforms.helpers.KlioUpdateAuditLog>` is a `Composite
Transform`_ that will update the audit log in the metadata of a :ref:`KlioMessage <klio-message>`
with the current job's :ref:`KlioJob`.

.. note::

    This transform is automatically called **unless** the event input is :ref:`configured to be
    skipped <skip-klio-read>`.


IO Helper Transforms
^^^^^^^^^^^^^^^^^^^^

``KlioTriggerUpstream``
~~~~~~~~~~~~~~~~~~~~~~~

``KlioTriggerUpstream`` is a `Composite Transform`_ that will trigger an upstream streaming job.
This is particularly useful when input data does not exist.

.. caution::

    Klio does not automatically trigger upstream jobs if input data does not exist. It must be used
    manually within a job's pipeline definition (in ``run.py::run``).


.. note::

    By default, Klio handles the input data existence check and only provides the ``run`` function
    in ``run.py`` a ``PCollection`` with ``KlioMessages`` of input data that has been found. In
    order to also have access to input not found, that default input data existence check must be
    turned off by setting :ref:`skip_klio_existence_check <skip-input-ext-check>` to ``True``. Then the input existence check
    must be invoked manually. See example ``run.py`` and ``klio-job.yaml`` files below.


.. code-block:: python

    # Example run.py
    import apache_beam as beam
    from klio.transforms import helpers
    import transforms

    def run(input_pcol, config):
        # use the default helper transform to do the default input check
        # in order to access the output tagged with `not_found`
        input_data = input_pcol | helpers.KlioGcsCheckInputExists()

        # Pipe the input data that was not found (using Tagged Outputs)
        # into `KlioTriggerUpstream` in order to update the KlioMessage
        # metadata, log it, then publish to upstream's
        _ = input_data.not_found | helpers.KlioTriggerUpstream(
            upstream_job_name="my-upstream-job",
            upstream_topic="projects/my-gcp-project/topics/upstream-topic-input",
            log_level="DEBUG",
        )

        # pipe the found input pcollection into other transform(s) as needed
        output_pcol = input_data.found | beam.ParDo(MyTransform())
        return output_pcol

.. code-block:: yaml
    :emphasize-lines: 7,23

    # Example klio-job.yaml
    version: 2
    job_name: my-job
    pipeline_options:
      project: my-gcp-project
      # `KlioTriggerUpstream` only supports streaming jobs
      streaming: True
      # <-- snip -->
    job_config:
      events:
        inputs:
          - type: pubsub
            topic: projects/my-gcp-project/topics/upstream-topic-output
            subscription: projects/my-gcp-project/subscriptions/my-job-input
        # <-- snip -->
      data:
        inputs:
          - type: gcs
            location: gs://my-gcp-project/upstream-output-data
            file_suffix: .ogg
            # Be sure to skip Klio's default input existence check in
            # order to access the input data that was not found.
            skip_klio_existence_check: True


``KlioWriteToEventOutput``
~~~~~~~~~~~~~~~~~~~~~~~~~~

:class:`KlioWriteToEventOutput <klio.transforms.helpers.KlioWriteToEventOutput>` is a `Composite
Transform`_ to write to the configured event output. The transform is currently available for
writing to ``file`` types and ``pubsub`` types.

.. code-block:: python

    class KlioGcsFilterOutput(beam.PTransform):
        """Klio composite transform to filter output data."""

        def expand(self, pcoll):
            # Check if output data exists
            output_exists = pcoll | "Output Exists Filter" >> KlioGcsCheckOutputExists()

            # Filter if existing output should be force-processed
            output_force = output_exists.found | "Force Filter" >> KlioFilterForce()

            # Handle items that should be sent directly to output
            _ = output_force.pass_thru | "Passthru Found Output" >> KlioWriteToEventOutput()


.. _transform-klio-drop:

``KlioDrop``
~~~~~~~~~~~~

:class:`KlioDrop <klio.transforms.helpers.KlioDrop>` is a `Composite Transform`_ that will simply
log and drop a ``KlioMessage``.

.. code-block:: python

    class KlioGcsFilterInput(beam.PTransform):
        """Klio composite transform to drop input data that is not found
        """

        def expand(self, pcoll):
            # Check if input data exists
            input_data = pcoll | "Input Exists Filter" >> KlioGcsCheckInputExists()

            # Drop the KlioMessage if data does not exist
            _ = input_data.not_found | "Drop Not Found Data" >> KlioDrop()

            # Do something with the found input data
            return input_data.found


Debugging Transforms
^^^^^^^^^^^^^^^^^^^^


``KlioDebugMessage``
~~~~~~~~~~~~~~~~~~~~

:class:`KlioDebugMessage <klio.transforms.helpers.KlioDebugMessage>` is a `Composite Transform`_
that will log a ``KlioMessage`` at the given point in a pipeline. It can be used any number of
times within a transform.

.. code-block:: python

    from klio.transforms import helpers

    def run(in_pcol, config):
        return (
            in_pcol
            | "1st debug" >> helpers.KlioDebugMessage()
            | MyTransform()
            | "2nd debug" >> helpers.KlioDebugMessage(prefix="[MyTransform Output]")
            | MyOtherTransform()
            | "3rd debug" >> helpers.KlioDebugMessage(
                prefix="[MyOtherTransform Output]", log_level="ERROR"
            )
        )

``KlioSetTrace``
~~~~~~~~~~~~~~~~

:class:`KlioSetTrace <klio.transforms.helpers.KlioSetTrace>` is a `Composite Transform`_ that will
insert a trace point (via :func:`pdb.set_trace`) at a given point in a pipeline.

.. code-block:: python

    from klio.transforms import helpers

    def run(in_pcol, config):
        return in_pcol | helpers.KlioSetTrace() | MyTransform()


.. _custom-existence-checks:

Custom Data Existence Checks
----------------------------

Klio by default handles these input and output existence checks. However Klio can also be
configured to skip these checks if custom control is desired.

To add custom checks, define a new transform that will hold custom existence checking logic.

.. code-block:: python

    # transforms.py file

    import apache_beam as beam


    class MyCustomInputExistenceDoFn(beam.DoFn):

        def process():
            pass


The built-in Klio existence checks make use of Beam's `Tagged Outputs`_ to output multiple
PCollections from a single transform or "tag" values with helpful labels for use in the pipeline.

.. code-block:: python

    # transforms.py file

    import apache_beam as beam

    from apache_beam import pvalue


    class CustomDataExistState(enum.Enum):

        # Note these values can be anything - not limited to (not) found tags
        FOUND = "found"
        NOT_FOUND = "not_found"


    class MyCustomInputExistenceDoFn(beam.DoFn):

        def process(kmsg):

            item = kmsg.data.v2.element

            item_exists = #  Do some custom logic here

            state = CustomDataExistState.FOUND
            if not item_exists:
                state = CustomDataExistState.not_found

            yield pvalue.TaggedOutput(state.value, kmsg.SerializeToString())



The custom existence check transform can then be imported and used as part of a composite transform:

.. code-block:: python

    # transforms.py file

    from transforms import MyCustomInputExistenceDoFn

    class MyCompositeTransform(beam.PTransform):
        """Klio composite transform to drop input data that is not found
        """

        def expand(self, pcoll):
            # Check if input data exists
            input_data = pcoll | "Custom Input Exists Filter" >> MyCustomInputExistenceDoFn()

            # Drop the KlioMessage if data does not exist
            _ = input_data.not_found | "Drop Not Found Data" >> KlioDrop()

            # Do something with the found input data
            return input_data.found

The composite transform can then be imported into the rest of the pipeline in the ``run.py`` file.

.. code-block:: python

    # run.py file


    from transforms import MyCompositeTransform


    def run(in_pcol, config):

        out_pcol = in_pcol | MyCompositeTransform()

        return out_pcol


.. _Composite Transform: https://beam.apache.org/documentation/programming-guide/#composite-transforms
.. _Tagged Outputs: https://beam.apache.org/documentation/programming-guide/#additional-outputs

.. _helper-transforms:

Implementing Transforms
=======================

.. _helper_transforms:

Helper Transforms
-----------------
Helper transforms aid existence checks, ping mode, and force re-generating output. These
transforms can be imported from ``klio.transforms.helpers``.

.. _data-existence-checks:

Data Existence Checks
^^^^^^^^^^^^^^^^^^^^^

For :ref:`data IO types <data-inputs-type>` of ``gcs``, Klio will perform the input
existence check for you. Data input and output existence checks are configured by the :ref:`existence check <skip-input-ext-check>`. These two configuration field
default to ``False`` and will therefore conduct transforms ``KlioGcsCheckInputExists``
and ``KlioGcsCheckOutputExists`` automatically. If :ref:`custom data existence
checks <custom-existence-checks>` are preferred then these fields should be set to ``True``.


``KlioGcsCheckInputExists`` and ``KlioGcsCheckOutputExists`` work by inspecting data configuration
for the fields for :ref:`data locations <data-inputs-location>` ``job_config.data.(in|out)puts[].location`` and :ref:`file suffix <data-inputs-file-suffix>` ``job_config.data.(in|out)puts[].file_suffix``. For
example, if we have an element that represents a track ID ``f00b4r``, Klio would inspect the
existence of the path: ``gs://foo-proj-input/example-streaming-parent-job-output/f00b4r.ogg``.


``KlioGcsCheckInputExists``
"""""""""""""""""""""""""""

``KlioGcsCheckInputExists`` is a `Composite Transform`_ to check the input data existence in GCS. The transform
utilizes `Tagged Outputs`_ to label output as either as ``not_found`` or ``found``.

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
""""""""""""""""""""""""""""

``KlioGcsCheckOutputExists`` is a composite transform to check the output exists in GCS. The
transform will tag output as either ``not_found`` or ``found``.

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
^^^^^^^^^^^^^^

``KlioFilterPing``
""""""""""""""""""

``KlioFilterPing`` is a composite transform to tag outputs if in :ref:`ping mode <ping-mode>` or not. The transform will tag output as either ``pass_thru`` or ``process``.


.. code-block:: python

    class MyGcsFilterToProcess(beam.PTransform):
        """Composite transform to filter pcollections for processing"""

        def expand(self, pcoll):
            ping_pcoll = pcoll | "Ping Filter" >> KlioFilterPing()

            # handle any items that should just be sent to output directly
            _ = ping_pcoll.pass_thru | "Passthru Ping" >> MyPassThruTransform()

            out_pcoll = ping_pcoll.process | "Process Data" >> MyPrcessTransform()


``KlioFilterForce``
"""""""""""""""""""

``KlioFilterForce`` is a composite transform to filter if existing output should be
:ref:`force-processed <force-mode>`. The transform will look at a job's configuration for whether or
not there is a global (pipeline-wide) forcing of messages with already-existing output. It will first inspect whether a message has an explicit ``True`` or ``False`` set for force processing. If
force mode is not set, then ``KlioFilterForce`` will inspect the pipeline configuration. The default
is ``False``. The ``KlioFilterForce`` transform will tag output as either ``pass_thru`` or
``process``.



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


IO Helpers
^^^^^^^^^^

``KlioWriteToEventOutput``
""""""""""""""""""""""""""

``KlioWriteToEventOutput`` is a composite to write to the configured event output. The transform is
currently available for writing to file types and pubsub types.

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
""""""""""""

``KlioDrop`` is a composite transform that will simply log and drop a KlioMessage.

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


.. _custom-existence-checks:

Custom Data Existence Checks
-------------------------------
Klio by default handles these input and output existence checks. However Klio can also be configured
to skip these checks if custom control is desired.

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

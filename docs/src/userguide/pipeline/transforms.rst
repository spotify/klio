Implementing Transforms
=======================

.. _helper_transforms:

Helper Transforms
---------------------
Helper transforms aid existence checks, ping mode, and force re-generating output. These
transforms can be imported from ``klio.transforms.helpers``.

Data Existence Checks
^^^^^^^^^^^^^^^^^^^^^^^

For ``gcs`` IO types, Klio will perform the input existence check for you. Data input and output
existence checks are configured by the configuration fields ``job_config.data.(in|out)puts[].skip_klio_existence_check``. These two configuration fields default to ``False`` and will therefore conduct
transforms ``KlioGcsCheckInputExists`` and ``KlioGcsCheckOutputExists`` automatically. If
:ref:`custom data existence checks <custom-existence-checks>` are preferred then
these fields should be set to ``True``.


``KlioGcsCheckInputExists`` and ``KlioGcsCheckOutputExists`` work by inspecting the configurations
for the fields ``job_config.data.(in|out)puts[].location`` and ``job_config.data.(in|out)puts[].file_suffix``. For example, if we have
an element that represents a track ID ``f00b4r``, Klio would inspect the existence of the path: ``gs://foo-proj-input/example-streaming-parent-job-output/f00b4r.ogg``.

.. todo::

    Might be nice to link to some config docs here


``KlioGcsCheckInputExists``
"""""""""""""""""""""""""""""

``KlioGcsCheckInputExists`` is a transform to check the input data existence in GCS. The transform
will tag output as either as ``not_found`` or ``found``.

.. code-block:: python

    class KlioGcsFilterInput(beam.PTransform):
        """Klio composite transform to filter input data"""

        def expand(self, pcoll):

            # Check if input data exists
            input_data = pcoll | "Input Exists Filter" >> KlioGcsCheckInputExists()

            # Do something with the data that does not exist
            _ = input_data.not_found | "Not Found Data Transform" >> MyTransform()

            # Do something with the data does exist
            return input_data.found

``KlioGcsCheckOutputExists``
""""""""""""""""""""""""""""""

``KlioGcsCheckOutputExists`` is a transform to check the output exists in GCS. The transform will
tag output as either ``not_found`` or ``found``.

.. code-block:: python

    class KlioGcsFilterOutput(beam.PTransform):
        """Klio composite transform to filter output data."""

        def expand(self, pcoll):
            # Check if output data exists
            output_exists = pcoll | "Output Exists Filter" >> KlioGcsCheckOutputExists()

            # Do something with output data that is found
            to_filter = output_exists.found | "Transform Found Data" >> MyTransformAlreadyFound()

            # Do somethng with the output data that is not found
            to_process = output_exists.not_found | "Data Not Found" >> MyTransformNotFound()


Data Filtering
^^^^^^^^^^^^^^^

``KlioFilterPing``
"""""""""""""""""""

``KlioFilterPing`` is a helper transform to tag outputs if in ping mode or not. The transform will
tag output as either ``pass_thru`` or
``process``.


.. code-block:: python

    class KlioGcsFilterToProcess(beam.PTransform):
        """Klio composite transform to filter pcollections for processing"""

        def expand(self, pcoll):
            ping_pcoll = pcoll | "Ping Filter" >> KlioFilterPing()

            # handle any items that should just be sent to output directly
            _ = ping_pcoll.pass_thru | "Passthru Ping" >> MyPassThruTransform()

            out_pcoll = ping_pcoll.process | "Process Data" >> MyPrcessTransform()


``KlioFilterForce``
""""""""""""""""""""

``KlioFilterForce`` helper transform to filter if existing output should be force-processed. The
transform will look at a job's configuration for whether or not there is a global (pipeline-wide)
forcing of messages with already-existing output. It will first inspect whether a message has an
explicit ``True`` or ``False`` set for force processing. If nothing is set, then ``KlioFilterForce`` will
look to what is configured (see configuration example). Default is `False`. The ``KlioFilterForce`` transform will tag output as either ``pass_thru`` (aka it should not be processed)
or as ``process``.

.. todo::

    Link to configuration docs here

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
^^^^^^^^^^^^^^^^^^^^^^^

``KlioWriteToEventOutput``
"""""""""""""""""""""""""""

``KlioWriteToEventOutput`` is a composite transform to write to the configured event output. The
transform is currently available for writing to file types and pubsub types.

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


``KlioDrop``
""""""""""""""

``KlioDrop`` is a transform that will simply log and drop a KlioMessage.

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


.. todo::

    How to implement own custom data existence check

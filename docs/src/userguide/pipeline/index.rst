Pipelines
=========

.. toctree::
   :maxdepth: 1
   :hidden:

   transforms
   context
   message
   state
   utilities
   multiple_inputs
   metrics

A `Beam Pipeline`_ encapsulates the various steps of the Klio job from reading input data,
transforming the data, and writing output data. Klio pipelines offer a Pythonic interface to
build upon beam pipelines and allow large-scale data processing on Docker and `Google
Dataflow`_.

A typical Klio job is a Python Dataflow job that read elements in the form of Klio messages from and input source.
These elements are a references to binary data, usually audio files. The corresponding audio file will be downloaded,
and the binary data can then be processed by applying transforms on the data. The output of the job can be uploaded to
another GCS location. Klio aims to simplify the code involved in these data pipelines.

Klio pipelines are organized into three structures - the :ref:`Klio Message<pipeline-overview-klio-message>`
which is a unit of work passed around as a trigger for the pipeline, the :ref:`run function <pipeline-overview-run-function>` which is the entry point for the pipeline and encapsulates the various DAG-like steps of the pipeline,
and the :ref:`transforms <pipeline-overview-transforms>` or the individual steps of the overall Klio pipeline.


Pipeline Structures
-------------------

.. _pipeline-overview-klio-message:

Klio Message
^^^^^^^^^^^^

A :ref:`Klio Message <klio-message>` is protobuf data that is passed between transforms
and represents a unit of work to be done by the transform. It carries an ``element`` value
that serves as a reference to the data that is accessed during a transform in a pipeline.
The Klio message also carries other data fields such as whether a message is in ping mode
and should not be processed in a transform and whether a message should be force processed
despite output already existing.


.. _pipeline-overview-run-function:

Run function
^^^^^^^^^^^^

The run function is the main entrypoint to run a job's transform(s). It contains the overall process of the pipeline,
laying out the transforms that the klio messages will pass through during the pipeline.
The second input is a reference to the Klio :ref:`job config <job-config>` object
with fields defined in the ``klio-job.yaml``. The ``run`` function is defined in the ``run.py`` file.
The inputs to the ``run`` function are an input `PCollection`_ that is returned by Klio transforms
that will read in data depending on the type specified in the :ref:`event config <event-config>`.


.. code-block:: python

    # run.py file

    import apache_beam as beam

    import transforms


    def run(input_pcol, config):
        output_pcol = input_pcol | beam.ParDo(transforms.MyKlioTransform())
        return output_pcol


A run function can contain a single or multiple transforms.
All transforms are based on beam transforms but transforms can be extened to add customized logic.
Klio also provides :ref:`helper transforms <helper-transforms>` to aid with existence checks, data loading etc.


.. _pipeline-overview-transforms:

Transforms
^^^^^^^^^^

`Transforms`_ hold the logic involved in processing data.They receive a Klio Message,
perform some logic,and yield the Klio message to the next transform of the pipeline.
Transforms are typically defined in a ``transforms.py`` file then imported into the ``run.py`` file for use in the pipeline.

Below is an example of a transform that inherits from Beam's DoFn.


.. code-block:: python

    # transforms.py

    import apache_beam as beam


    class MyPlainTransform(beam.DoFn):
        def setup(self):
            # Some setup logic here
            self.audio_downloader = SomeAudioDownloader()

        def process(self, item):
            # <!-- snip -->

            yield item

Klio enhances Beam by offering decorators that can be imported from ``klio.transforms``
then used then decorating methods on transforms to make use of functionalities such as the examples below.

 * :ref:`De/serialization of Klio Messages <serialization-klio-message>`
 * :ref:`Inject klio context on methods and functions <accessing-klio-context>`
 * :ref:`Handle timeouts <timeout>`



.. code-block:: python

    # transforms.py

    import apache_beam as beam

    from klio.transforms import decorators


    class MyKliofiedTransform(beam.DoFn):
        @decorators.set_klio_context
        def setup(self):
            self.inputs = self._klio.job_config.inputs

        @decorators.handle_klio
        def process(self, item):
            entity_id = item.element.decode("utf-8")
            self._klio.logger.info(
                "Pocessing entity with ID %s" % (entity_id)
            )

            # some other logic
            yield item


Custom transforms can be imported and used in the ``run.py`` to put together the pipeline.

.. code-block:: python

    # run.py file

    import apache_beam as beam

    import transforms


    def run(input_pcol, config):
        output_pcol = input_pcol | beam.ParDo(transforms.MyKliofiedTransform())
        return output_pcol


Klio also offers composite :ref:`helper transforms <helper-transforms>` that can be used directly in the ``run.py`` function.
Helper transforms available including the below.

 * :ref:`Data existence cheks <data-existence-checks>`
 * :ref:`Inject klio context on methods and functions <accessing-klio-context>`
 * :ref:`Handle timeouts <timeout>`



Top Down and Bottom Up Execution
--------------------------------


Streaming Klio jobs are structured as directed acyclic graphs (DAGs) where parent jobs can trigger dependent child jobs.
Klio support two modes of execution - :ref:`top-down <top-down>` and :ref:`bottom-up <bottom-up>`.
Top-down execution is used when every step of the DAG should run for ever received klio message.
Bottom-up execution is used to run a single job for a file and mising upstream dependencies will be recursively created.





.. _Google Dataflow: https://cloud.google.com/dataflow
.. _Beam Pipeline: https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline
.. _PCollection: https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/values/PCollection.html
.. _Transforms: https://beam.apache.org/documentation/programming-guide/#applying-transforms
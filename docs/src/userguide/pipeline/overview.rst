Overview
========

.. _pipeline-overview-klio-message:

Klio Message
------------

A :ref:`Klio Message <klio-message>` is protobuf data that is passed between transforms
and represents a unit of work to be done by the transform. It carries an ``element`` value
that serves as a reference to the data that is accessed during a transform in a pipeline.
The Klio message also carries other data fields such as whether a message is in ping mode
and should not be processed in a transform and whether a message should be force processed
despite output already existing.


.. _pipeline-overview-run-function:

Run function
------------

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
All transforms are based on beam transforms but transforms can be extended to add customized logic.
Klio also provides :ref:`built-in transforms <builtin-transforms>` to aid with existence checks, data loading etc.


.. _pipeline-overview-transforms:

Transforms
----------

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
* :ref:`Retry on failure <retries>`



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


Klio also offers composite :ref:`built-in transforms <builtin-transforms>` that can be used directly in the ``run.py`` function.

* :ref:`Data existence checks <data-existence-checks>`
* :ref:`Inject klio context on methods and functions <accessing-klio-context>`
* :ref:`Handle timeouts <timeout>`
* :ref:`Retry on failure <retries>`



.. _Google Dataflow: https://cloud.google.com/dataflow
.. _Beam Pipeline: https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline
.. _PCollection: https://beam.apache.org/releases/javadoc/2.1.0/org/apache/beam/sdk/values/PCollection.html
.. _Transforms: https://beam.apache.org/documentation/programming-guide/#applying-transforms

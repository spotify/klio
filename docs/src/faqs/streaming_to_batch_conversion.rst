.. _batch-conversion:

Converting a Streaming Pipeline to Batch
========================================

Batch Klio pipelines can be useful when performing backfills or
when work can be done on a cadence. Batch jobs can also simplify local testing
as resources to handle Pub/Sub messages are not required to be set up to kick off a job.

Config Changes
--------------

In the ``klio-job.yaml`` there are two config values that need to change
in order to convert a streaming job to a batch job:
the :ref:`streaming <kliopipelineconfig-streaming>` field
and the :ref:`job_config.event <event-config>` input and output configurations.

Streaming
^^^^^^^^^

Set ``streaming`` to ``False``:

.. code-block:: yaml
   :emphasize-lines: 3

    name: my-stream-job-that-i-want-to-be-batch
    pipeline_options:
      streaming: False
    job_config:
      <-- snip -->


Event I/O
^^^^^^^^^

Currently the only supported event inputs and outputs for streaming jobs are Google Cloud Pub/Sub.
However there are multiple supported :ref:`event configurations <event-config>`
in batch mode, the simplest of which is a text file located locally or in GCS.
Similarly, writing event outputs to a GCS file is available in batch mode by setting
``job_config.event.outputs``.

An example of of the changes for reading and writing to a GCS file are seen below:

.. code-block:: yaml
   :emphasize-lines: 8,9,11,12

    name: my-stream-job-that-i-want-to-be-batch
    pipeline_options:
      streaming: False
      <-- snip -->
    job_config:
      event:
        inputs:
          - type: file
            location: gs://my-event-input/my-input-elements.txt
        outputs:
          - type: file
            location: gs://my-event-output/


.. note::

    A batch job can also be converted into a streaming job in a similar matter.
    However, missing resources such as Pub/Sub topics and subscriptions will
    need to be created with the command ``klio job verify --create-resources``.
I/O
===

Klio makes an important distinction between **Event I/O** and **Data I/O**.


Between input/output and event/data, the minimum required I/O for a Klio job is
a single event input.  Whether a job has event output or any data I/O depends
on the specifics of the job.

Inputs
-------

Event inputs correspond to individual events that indicate work has to be done,
whereas data inputs correspond to the data items that are the subject of the
work.  For example, let's say our ``klio-job.yaml`` has the following config:

.. code-block:: yaml

  job_config:
    events:
      inputs:
        - type: pubsub
          topic: my-pubsub-topic
          subscription: my-pubsub-subscription
    data:
      inputs:
        - type: gcs
          file_suffix: ".mp3"
          location: gs://my-input-bucket



Notice there are two inputs defined, one event input and one data input.  When
started, this Klio job will subscribe to the Pub/Sub topic ``my-pubsub-topic``
and listen for messages, with each message pointing to an audio file that needs
to be processed.  The audio file itself is not included in the message, but
instead is stored in the GCS bucket ``my-input-bucket``.


By default, Klio will make some assumptions about the correspondence between
event and data input items.  For example, if it receives a message (which
usually is a serialized :doc:`KlioMessage <../pipeline/message>`) with the
``element`` field set to the string ``"foobar"``, Klio will then use the
GCS data input to look for a file named ``gs://my-input-bucket/foobar.mp3``.
If that file does not exist, Klio will recognize the required data is missing
for this message and drop it without processing.

This behavior can be controlled with the ``skip_klio_existence_check`` config
option:

.. code-block:: yaml

  job_config:
    data:
      inputs:
        - type: gcs
          skip_klio_existence_check: true
          file_suffix: ".mp3"
          location: gs://my-input-bucket

When set to ``true``, Klio will no longer automatically do such checks,
leaving you the option to implement the existence check yourself or to skip
such a check entirely.

.. caution::

  Be aware that when using the built-in existence checks, Klio will check for
  the input file's existence but it will *not* take any action to download the
  file on its own.  This is generally a responsibility of your own transform,
  since only you know exactly where in your job you actually need the file.

In some cases you may not want Klio to automatically handle reading from an
event input.  This can be controlled by setting ``skip_klio_read`` to ``true``
in the input's config.  Be aware this will affect the call to your job's
``run`` function.  Normally the function is passed a ``PCollection``, but when
``skip_klio_read`` is ``true``, it will instead be passed a ``pipeline``
object, and it will be the responsibility of your own code to setup the input.

.. caution::

   When ``skip_klio_read`` is set to ``true``, a number of other features
   normally handled by Klio will be disabled:

   * Filtering events for intended recipients (part of bottom-up execution)
   * updates to the audit log section of ``KlioMessage`` events
   * detecting if a message is in ping mode
   * any automatic data input existence checks (described above)
   * any automatic data output existence checks (described below)


.. _multiple-event-inputs:

Multiple Event Inputs
^^^^^^^^^^^^^^^^^^^^^

Currently, Klio supports working with multiple event inputs.
This may be multiple Pub/Sub subscriptions, or BigQuery tables, or any other of Klio's :ref:`supported event inputs <supported-event-io>`.
A job may not have multiple inputs that support different modes.
For instance, Pub/Sub is only supported in streaming mode.
Therefore, a streaming job may not mix reading from a Pub/Sub subscription with any inputs supported in batch mode.

Step 1: Defining Multiple Inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To define multiple event inputs, just add another list item under ``job_config.events.inputs`` in the ``klio-job.yaml`` file:

.. code-block:: yaml

  job_config:
    events:
      inputs:
        - type: file
          location: ./batch_track_ids_1.txt
        - type: file
          location: ./batch_track_ids_2.txt
    # <-- snip -->

Klio will automatically read from the configured inputs concurrently.

Step 2: Using Multiple Inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the job's ``run.py`` file, the ``run`` function will be called with *multiple* :class:`PCollections <apache_beam.pvalue.PCollection>` instead of a single ``PCollection`` as the first argument.
The first argument, ``pcolls``, is a :func:`namedtuple <collections.namedtuple>` which length is equal to the number of configured inputs.
To access the input ``PCollection`` for a particular configured input, names are generated using the configured inputs ``type`` plus its index (using zero-indexing).
For example:

.. code-block:: py
  :emphasize-lines: 2,3

  def run(pcolls, config):
      first = pcolls.file0 | "process first" >> beam.Map(first_func)
      second = pcolls.file1 | "process second" >> beam.Map(second_func)
      combined = (first, second) | beam.Flatten()
      return combined | "process combined" >> beam.Map(combined_func)


If needed, the configuration is attached to the :class:`KlioContext <klio.transforms.core.KlioContext>` object:

.. code-block:: py

  @decorators.handle_klio
  def my_map_function(ctx, item):
      file0_config = ctx.config.job_config.events.inputs[0]
      file1_config = ctx.config.job_config.events.inputs[1]
      ...


Outputs
-------

Likewise event and data outputs correspond to the output produced by a job.
For example:

.. code-block:: yaml

  job_config:
    events:
      inputs:
        # ...
      outputs:
        - type: pubsub
          topic: my-output-pubsub-topic
    data:
      inputs:
        # ...
      outputs:
        - type: gcs
          file_suffix: ".wav"
          location: gs://my-output-bucket


This may represent a Klio job that transcodes audio files and writes the output
files to a GCS bucket while publishing events for each file written to Pub/Sub.

Like with data input, Klio will by default make similar assumptions about data
outputs.  In this example, if Klio detects that the output file already exists, it
will assume the input event was a duplicate and will drop the message without
processing it.  Again, setting ``skip_klio_existence_check`` in the data
output's config will disable this automatic check.

In some cases, you may want to have an event output configured but avoid having
Klio automatically writing an output message.  This could be the case if you
have multiple event outputs or want to customize the behavior of writing output
events.  In these situations, you can disable Klio's built-in writing of output
events by setting ``skip_klio_write`` to ``true`` in the event output's config:

.. _supported-event-io:

Event I/O
---------

Google Pub/Sub
^^^^^^^^^^^^^^

Currently Pubsub is the only supported event I/O in streaming jobs.

Event input items can be one of two formats.  The standard format is Klio's
:doc:`KlioMessage <../pipeline/message>` protobuf object, whose
``data.element`` field contains the value that is used by your job's
transforms.  In other words, the ``PCollection`` passed to your ``run.py``
contains only the contents of ``data.element``.  Alternatively, if
``allow_non_klio_messages`` is enabled in your job's config, Klio will accept
messages of any format and then hand off the entire message for your transforms
to process.

More information about configuring pub/sub can be found in the
:ref:`event-config-pubsub` event config section.

Google BigQuery
^^^^^^^^^^^^^^^

Only supported in batch mode.

Klio supports reading from and writing to `Google BigQuery <https://cloud.google.com/bigquery/docs>`_ tables as event inputs/outputs.

Read more about configuring for :ref:`reading events from BigQuery <event-input-config-bigquery>` and :ref:`writing events to BigQuery <event-output-config-bigquery>`.


Text Files
^^^^^^^^^^

Only supported in batch mode.

Klio supports reading events from and writing events to text files.
When reading, each line represents its own ``KlioMessage``, and the data on the line is converted to :ref:`KlioMessage.data.element <data>`.
When writing, each :ref:`KlioMessage.data.element <data>` of a ``KlioMessage`` is written to its own line.

Files can be read & written locally (supported in Direct Runner only), or from/to Google Cloud Storage (supported for both Direct Runner and Dataflow Runner).

Read more about configuring for :ref:`reading events from files <event-input-config-files>` and :ref:`writing events to files <event-output-config-files>`.

Avro
^^^^

Only supported in batch mode.
Both local and GCS avro files are supported.

More information and examples on how to read and write to avro files
in :ref:`event-config-avro-read` the event config section.

Data I/O
--------

Google Cloud Storage
^^^^^^^^^^^^^^^^^^^^

Currently GCS is the only supported data I/O in streaming jobs.  All data input
items reside in a GCS bucket and are expected to have the same file suffix.

More information about configuring GCS input can be found in the
:ref:`data-config-gcs` data config section.

Local Files (Direct Runner Only)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using ``direct-runner`` for dev/testing, local files can also be used with
the ``file`` data I/O type.




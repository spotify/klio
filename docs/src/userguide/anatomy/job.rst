Overview
========

.. _overview-streaming-job:

Streaming Jobs
--------------


A typical **streaming** Klio job is a Python Dataflow job running Dockerized workers that:

1. reads messages from a `Pub/Sub`_ input subscription (potentially subscribed to the output topic of another job) where each message has a reference to binary data;
2. downloads that binary data from a `GCS`_ bucket;
3. processes the downloaded data by applying transforms (which may include running Python libraries with C extensions, Fortran, etc);
4. uploads the derived data to another `GCS`_ bucket;
5. writes a message to a single `Pub/Sub`_ output topic so downstream Klio jobs may process the derived data.

Here’s an overview diagram of how this works:

.. figure:: images/job_overview.png
    :alt: klio job overview diagram


The above architecture overview mentions a few resources in Google Cloud that a typical streaming
Klio job needs. While Dataflow handles the execution of a job, Klio makes use of Pub/Sub and GCS
buckets to create a :doc:`DAG <graph>` to string job dependencies together, allowing for
:ref:`top-down <top-down>` and :ref:`bottom-up <bottom-up>` execution.

Batch Jobs
----------

A typical **batch** Klio job is the batch analogue of a :ref:`Klio streaming job <overview-streaming-job>`.

It is a Python Dataflow job running Dockerized workers that:

1. Reads an item of input from a batch source (such as a text file, a `GCS`_ file, or a `BigQuery`_ table), where each input is a reference to binary data;
2. downloads that binary data from a `GCS`_ bucket;
3. processes the downloaded data by applying transforms (which may include running Python libraries with C extensions, Fortran, etc);
4. uploads the derived data to another `GCS`_ bucket;
5. writes a message to a sink (such as a text file, a `GCS`_ file, a `BigQuery`_ table, or even a `Pub/Sub`_ output topic).

Note that :ref:`top-down <top-down>` and :ref:`bottom-up <bottom-up>` execution have been built to support streaming jobs. We recommend using a separate orchestration framework, such as `Luigi`_, for creating and coordinating a DAG of batch Klio jobs.

.. _Pub/Sub: https://cloud.google.com/pubsub/docs
.. _GCS: https://cloud.google.com/storage/docs
.. _BigQuery: https://cloud.google.com/bigquery/docs
.. _Luigi: https://github.com/spotify/luigi

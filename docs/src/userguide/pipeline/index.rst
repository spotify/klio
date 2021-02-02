The Klio Pipeline
=================


.. the toctree is shown twice; the 2nd time is so it renders where we want the list of contents
    on the page itself, but it doesn't show in the sidebar. For some reason, it needs to be above
    the content (with `:hidden:`) to be shown in the sidebar :eye_roll:

.. toctree::
   :maxdepth: 1
   :hidden:

   overview
   transforms
   utilities
   context
   message
   state
   metrics
   io
   multiple_inputs

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


Top Down and Bottom Up Execution
--------------------------------

Streaming Klio jobs are structured as directed acyclic graphs (DAGs) where parent jobs can trigger dependent child jobs.
Klio support two modes of execution - :ref:`top-down <top-down>` and :ref:`bottom-up <bottom-up>`.
Top-down execution is used when every step of the DAG should run for ever received klio message.
Bottom-up execution is used to run a single job for a file and missing upstream dependencies will be recursively created.




Further Documentation
---------------------

.. toctree::
   :maxdepth: 1

   overview
   transforms
   utilities
   context
   message
   state
   metrics
   multiple_inputs


.. _Beam Pipeline: https://beam.apache.org/documentation/programming-guide/#creating-a-pipeline
.. _Google Dataflow: https://cloud.google.com/dataflow

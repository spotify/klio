Reference
=========

.. toctree::
   :maxdepth: 2
   :hidden:

   cli/index
   lib/index
   Audio Helpers <audio/index>
   Executor <executor/index>
   Core <core/index>
   Dev Tools <devtools/index>

.. include:: /.current_status.rst

This reference guide contains a detailed description of the API of all the Klio libraries. The
reference describes how the helper transforms, decorators, and other public functionality works and
which parameters can be used. It assumes an understanding of key concepts of Klio and `Apache
Beam`_.

For getting up to speed on Apache Beam, check out `this overview`_, walk through the `Beam
Quickstart for Python`_, and work through Beam's `word count tutorial`_. There is also a talk on
`Streaming data processing pipelines in Python with Apache Beam
<https://www.youtube.com/watch?v=I1JUtoDHFcg>`_ (YouTube video).

Ecosystem
---------

The Klio ecosystem is made up of multiple, separate Python packages, some of which are
user-facing.

User Facing
^^^^^^^^^^^

* :doc:`klio-cli <cli/index>`: The main CLI entrypoint. This CLI is used for creating, deploying, testing, and profiling of Klio jobs, among other helpful commands.
* :doc:`klio <lib/index>`: The required library for implementing Klio-ified transforms with :doc:`helpers <../userguide/pipeline/utilities>` and make use of the :ref:`message-handling logic <msg-proc-logic>`. Import this library to Klio-ify the `Beam transforms`_ in a pipeline.
* :doc:`klio-audio <audio/index>`: An optional library with helper transforms related to processing audio, including downloading from `GCS`_ into memory, loading into `numpy`_ via `librosa`_, generate various spectrograms, among others.

Internals
^^^^^^^^^

The following internal packages are not meant for explicit, public usage with a Klio job. Use these
libraries **at your own risk** as the APIs and functionality may change.

* :doc:`klio-exec <executor/index>`: The executor is a CLI (Apache Beam's `"driver"`_) that launches a pipeline from within a job's Docker container. Many commands from the ``klio-cli`` directly wrap to commands in the executor: a ``klio-cli`` command will set up the Docker context needed to correctly run the pipeline via the associated command with ``klio-exec``. The Docker context includes mounting the job directory, sets up environment variables, mounting credentials, etc.
* :doc:`klio-core <core/index>`: A library of common utilities, including the Klio :ref:`protobuf definitions <proto-defs>` and configuration parsing.


Other
^^^^^

* :doc:`klio-devtools <devtools/index>`: A collection of utilities to help aid the development of Klio. This is not meant to be used by users.


.. _"driver": https://beam.apache.org/documentation/programming-guide/#overview
.. _Apache Beam: https://beam.apache.org/documentation/
.. _this overview: https://beam.apache.org/get-started/beam-overview/
.. _Beam Quickstart for Python: https://beam.apache.org/get-started/quickstart-py/
.. _word count tutorial: https://beam.apache.org/get-started/wordcount-example/
.. _Beam transforms: https://beam.apache.org/documentation/programming-guide/#transforms
.. _GCS: https://cloud.google.com/storage/docs
.. _numpy: https://numpy.org/
.. _librosa: https://librosa.org/

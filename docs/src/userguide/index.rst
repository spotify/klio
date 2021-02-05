Hello, Klio!
============

.. toctree::
    :maxdepth: 2
    :hidden:

    intro
    quickstart/index
    anatomy/index
    pipeline/index
    config/index
    examples/index


.. include:: ../../../README.rst
    :start-after: start-intro
    :end-before: end-intro

.. include:: /.current_status.rst

Why Klio?
---------

Klio enables:

* Organizations to build media processing systems that :violetemph:`share tooling & infrastructure` between production systems and research teams;
* An architecture that encourages :violetemph:`reusable` jobs and :violetemph:`shared` outputs, lowering maintenance and recomputation costs;
* Continuous, :violetemph:`event-driven processing` of rapidly growing catalogues of content.

It lets organizations that do their own research (e.g. machine learning on audio) move faster by enabling:

* Researchers to benefit from working with the :violetemph:`same infrastructure` as engineers responsible for production systems as well as :violetemph:`processing power` for complete catalogues of content;
* Engineers have a :violetemph:`simpler framework` to directly productionize large media processing jobs produced by researchers when they’re already using the same frameworks for development;
* Organizations to benefit from these pipelines immediately as they add new content, allowing new content to be :violetemph:`processed immediately` on ingestion through a streaming pipeline and for backfills to be handled as a batch job with the :violetemph:`same code`.

What are Klio Pipelines?
------------------------

A Klio pipeline:

* use :violetemph:`reference identifiers` to audio files from event inputs (e.g. `Google Pub/Sub`_),
* download those files onto worker machines,
* run :violetemph:`processing algorithms` (e.g. `librosa`_, `ffmpeg`_, trained ML models, anything) over these files,
* then save the resulting output to the data store of choice.

Processing algorithms can be ML-based or otherwise, as long as they are defined or otherwise can be wrapped in Python. Klio jobs can scale up to process an entire corpus of media, or down to a single item for fast iteration.

:violetemph:`Streaming` Klio jobs are wired up via :violetemph:`a messaging queue` [#fn1]_ to create complex processing :doc:`directed acyclic graphs <anatomy/graph>` - even across teams, much like backend services.


Why is it called Klio?
----------------------

Kleio (latinized to `Clio <https://en.wikipedia.org/wiki/Clio>`_) is the Greek muse of history and lyre playing.
Klio (we dropped the `e` for easier spelling) takes inspiration from Kleio's ability to inscribe historical significance to the actions of the present and to recall historical meaning from ancient works.

The name Klio also pairs nicely with another Spotify open source project, `Scio <https://github.com/spotify/scio>`_, that brings Apache Beam to Scala.

More Questions?
---------------

.. include:: ../../../README.rst
    :start-after: start-resources
    :end-before: end-resources

.. rubric:: Footnotes

.. [#fn1] Currently, streaming Klio jobs only support reading from/writing to `Google Pub/Sub`_. We welcome :doc:`contributions <../contributors>` for expanding Klio's streaming I/O support of `Beam's built-in I/O transforms`_.

.. _Apache Beam: https://beam.apache.org/
.. _Google Pub/Sub: https://cloud.google.com/pubsub/docs/overview
.. _librosa: https://librosa.org/
.. _ffmpeg: https://ffmpeg.org/
.. _Beam's built-in I/O transforms: https://beam.apache.org/documentation/io/built-in

Quick Start
===========

.. toctree::
    :maxdepth: 1
    :hidden:

    installation
    hello_klio_streaming
    hello_klio_batch

.. include:: /.current_status.rst

Before we begin, it will be helpful to understand the basics of `Apache Beam's Python SDK`_ .
For streaming pipelines, a basic understanding of of `Google Pub/Sub`_ is also helpful before
getting started.

Specifically, you should know the following topics:

* What Beam :violetemph:`PCollections` and :violetemph:`PTransforms` are, and how to write :violetemph:`Beam pipelines`. Learn by reading `this overview`_, walking through the `Beam Quickstart for Python`_, and working through Beam's `word count tutorial`_. There is also a talk on `Streaming data processing pipelines in Python with Apache Beam`_ (YouTube video).
* How to :violetemph:`launch a Beam job on Dataflow` (a runner for Beam jobs).
  You can familiarize yourself with this `quickstart`_.
* (Streaming Pipelines) What are Pub/Sub :violetemph:`topics`, :violetemph:`subscriptions` and
  :violetemph:`messages`, and how they work. This `Pub/Sub overview`_ may be helpful, as well as
  this `interactive tutorial`_.


All set? Let's get started with the :doc:`installation <installation>`!

.. _Apache Beam's Python SDK: https://beam.apache.org/documentation/sdks/python/
.. _Google Pub/Sub: https://cloud.google.com/pubsub
.. _Pub/Sub overview: https://cloud.google.com/pubsub/docs/overview
.. _interactive tutorial: https://console.cloud.google.com/cloudpubsub?tutorial=pubsub_quickstart
.. _this overview: https://beam.apache.org/get-started/beam-overview/
.. _Beam Quickstart for Python: https://beam.apache.org/get-started/quickstart-py/
.. _word count tutorial: https://beam.apache.org/get-started/wordcount-example/
.. _Streaming data processing pipelines in Python with Apache Beam: https://www.youtube.com/watch?v=I1JUtoDHFcg
.. _quickstart: https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

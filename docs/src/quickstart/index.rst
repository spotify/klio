Quick Start
===========

.. toctree::
    :maxdepth: 1
    :hidden:

    installation
    hello_klio

Before we begin, it will be helpful to understand the basics of `Google Pub/Sub`_ and `Apache Beam's Python SDK`_ before getting started.

Specifically, you should know the following topics:

* What are Pub/Sub :greenemph:`topics`, :greenemph:`subscriptions` and :greenemph:`messages`, and how they work. This `Pub/Sub overview`_ may be helpful, as well as this `interactive tutorial`_.
* What Beam :blueemph:`PCollections` and :blueemph:`PTransforms` are, and how to write :blueemph:`Beam pipelines`. Learn by reading `this overview`_, walking through the `Beam Quickstart for Python`_, and working through Beam's `word count tutorial`_. There is also a talk on `Streaming data processing pipelines in Python with Apache Beam`_ (YouTube video).
* How to :greenemph:`launch a Beam job on Dataflow` (a runner for Beam jobs). You can familiarize yourself with this `quickstart`_.

All set? Let's get started with the :doc:`installation <installation>`!

.. _Google Pub/Sub: https://cloud.google.com/pubsub
.. _Apache Beam's Python SDK: https://beam.apache.org/documentation/sdks/python/
.. _Pub/Sub overview: https://cloud.google.com/pubsub/docs/overview
.. _interactive tutorial: https://console.cloud.google.com/cloudpubsub?tutorial=pubsub_quickstart
.. _this overview: https://beam.apache.org/get-started/beam-overview/
.. _Beam Quickstart for Python: https://beam.apache.org/get-started/quickstart-py/
.. _word count tutorial: https://beam.apache.org/get-started/wordcount-example/
.. _Streaming data processing pipelines in Python with Apache Beam: https://www.youtube.com/watch?v=I1JUtoDHFcg
.. _quickstart: https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python

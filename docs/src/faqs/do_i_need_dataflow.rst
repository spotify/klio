Do I need Google Cloud Dataflow to use Klio?
============================================

Klio is being developed and is tested against `Google Cloud Dataflow`_ on Google Cloud Platform, however the underlying Apache Beam project is designed to run workloads agnosicly across any data workflow engine.
Current `limitations with Beam Python`_ prevent all of its features from being used on every engine, however we expect increased compatibility as Apache Beam extends its underlying compatibility with these engines.


.. _Google Cloud Dataflow: https://cloud.google.com/dataflow
.. _limitations with Beam Python: https://beam.apache.org/documentation/runners/capability-matrix/

How does Klio relate to Apache Beam?
====================================

Klio is built on top of Apache Beam for Python.
By default, Klio provides an :violetemph:`opinionated` way to use Apache Beam for common media processing use cases, but allows the use of core Python Beam at any time if Klio’s opinions don't fit your use case.

Klio aims to be a more :violetemph:`Pythonic` implementation of the Beam framework, but it also offers several advantages over traditional Python Beam for media processing.
Klio provides a substantial reduction in boilerplate code (an average of 60%), a focus on heavy file I/O, and provides existing standards for connecting multiple streaming jobs together in a :doc:`DAG <../userguide/anatomy/graph>` (with :ref:`top-down<top-down>` and :ref:`bottom-up<bottom-up>` execution).
This allows teams to immediately focus on writing new pipelines, with the knowledge that they can easily be extended and connected later.

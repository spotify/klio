The ``klio-exec`` Package
=========================


.. image:: https://img.shields.io/pypi/v/klio-exec?color=%2300aa55&label=klio-exec
   :target: https://pypi.org/project/klio-exec
   :alt: Latest version of klio-exec on PyPI

.. image:: https://github.com/spotify/klio/workflows/klio-exec%20unit%20tests/badge.svg
   :target: https://github.com/spotify/klio/actions?query=workflow%3A%22klio-exec+unit+tests%22
   :alt: Status of klio-exec unit tests

.. start-klio-exec-intro

The executor – **not** meant to be used directly by the user – is a CLI that launches a pipeline from within a job's Docker container (a.k.a. an Apache Beam's `"driver"`_).
Many commands from the ``klio-cli`` directly wrap to commands in the executor: a ``klio-cli`` command will set up the Docker context needed to correctly run the pipeline via the associated command with ``klio-exec``.
The Docker context includes mounting the job directory, sets up environment variables, mounting credentials, etc.

As the ``klio-exec`` package is **not** meant to be installed directly, check out the `installation guide <https://docs.klio.io/en/latest/quickstart/installation.html>`_ for how to setup installation.
There is also the `user guide <https://docs.klio.io/en/latest/userguide/index.html>`_ and the `API documentation <https://docs.klio.io/en/latest/reference/executor/index.html>`_ for more information.


.. _"driver": https://beam.apache.org/documentation/programming-guide/#overview

The ``klio-audio`` Library
==========================

.. image:: https://img.shields.io/pypi/v/klio-audio?color=%2300aa55&label=klio-audio
   :target: https://pypi.org/project/klio-audio
   :alt: Latest version of klio-audio on PyPI

.. image:: https://github.com/spotify/klio/workflows/klio-audio%20unit%20tests/badge.svg
   :target: https://github.com/spotify/klio/actions?query=workflow%3A%22klio-audio+unit+tests%22
   :alt: Status of klio-audio unit tests

.. start-klio-audio-intro

``klio-audio`` is an optional library with helper transforms related to processing audio, including downloading from `GCS`_ into memory, loading into `numpy`_ via `librosa`_, generate various spectrograms, among others.

.. admonition:: Installation
    :class: tip

    To make use of ``klio-audio``, add ``klio[audio]`` in your ``job-requirements.txt`` file so that it is installed in your job's Docker image.

As the ``klio-audio`` library is not meant to be installed directly, check out the `installation guide <https://klio.readthedocs.io/en/latest/quickstart/installation.html>`_ for how to setup installation.
There is also the `user guide <https://klio.readthedocs.io/en/latest/userguide/index.html>`_ and the `API documentation <https://klio.readthedocs.io/en/latest/reference/audio/index.html>`_ for more information.


.. _GCS: https://cloud.google.com/storage/docs
.. _numpy: https://numpy.org/
.. _librosa: https://librosa.org/


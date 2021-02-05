.. _hello-klio-streaming:

Hello Klio Streaming Example
============================

This guide will show you how to set up your development environment to implement Klio,
create an example Klio streaming job and run it on DirectRunner. If you are interested in building
a streaming Klio job then checkout the :ref:`Klio Batch Quickstart Guide<hello-klio-batch>`.


.. attention::

    Be sure to follow the :doc:`installation instructions <installation>` before continuing on.


Create a New Klio Streaming Job
-------------------------------

:violetemph:`First`, initialize the ``klio_quickstart`` project directory for ``git``:

.. code-block::

    $ git init

.. caution::

    If the |klio-cli|_ was installed via :ref:`option 2 <install-option-2>` or :ref:`option 3 <install-option-3>`, make sure you're ``klio-cli`` virtualenv is activated.


:violetemph:`Next`, within your project directory, run the following command:

.. code-block:: sh

    $ klio job create --job-name klio-quick-start --create-resources --use-defaults


After responding to the prompts, Klio will:

1. Create a GCS bucket in the provided GCP project you provided in the prompt for output data: ``gs://$GCP_PROJECT-output``.
2. Create a `Google Stackdriver`_ dashboard in the provided GCP project for you to monitor runtime job metrics.
3. Create required files within the current working directory.
4. Create two Pub/Sub topics, one for input and one for output, in the provided GCP project: ``projects/$GCP_PROJECT/topics/klio-quick-start-input`` and ``projects/$GCP_PROJECT/topics/klio-quick-start-output``.
5. Create one Pub/Sub subscription to the input Pub/Sub topic in the provided GCP project: ``projects/$GCP_PROJECT/subscription/klio-quick-start-input-klio-quickstart-input``.

:violetemph:`Then`, commit the created job files into ``git``:

.. code-block::

    $ git add .
    $ git commit -m "Initial commit for Klio quickstart example"


Run the New Klio Job
--------------------

.. caution::

    If the |klio-cli|_ was installed via :ref:`option 2 <install-option-2>` or :ref:`option 3 <install-option-3>`, make sure you're ``klio-cli`` virtualenv is activated.

:violetemph:`First`, to run the job using DirectRunner:

.. code-block::

    $ klio job run --direct-runner


Klio will first build a Docker image of the example job with the required dependencies, then start the job locally.
To know it started successfully, you should see a log line containing

.. code-block:: text

    Running pipeline with DirectRunner

:violetemph:`Next`, in another terminal:

.. code-block::

    # within the project directory, with ``klio-cli`` virtualenv activated if needed
    $ klio message publish hello


This will create a :ref:`Klio message <klio-message>` that the job consumes and processes.
When the message was successfully consumed, you should see a log line of

.. code-block:: text

    Received 'hello' from Pub/Sub topic 'projects/$GCP_PROJECT/topics/klio-quick-start-input'

.. todo::

    Continue this example on adding (audio) data to read, running on dataflow, etc.


.. _Google Stackdriver: https://cloud.google.com/stackdriver/docs

.. there's no way to do nested formatting within the prose, so we have to do it this way
    https://docutils.sourceforge.io/FAQ.html#is-nested-inline-markup-possible

.. |klio-cli| replace:: ``klio-cli``
.. _klio-cli: https://pypi.org/project/klio-cli

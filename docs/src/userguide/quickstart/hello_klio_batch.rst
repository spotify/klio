.. _hello-klio-batch:

Hello Klio Batch Example
========================

This guide will show you how to set up your development environment to implement Klio,
create an example Klio batch job and run it on DirectRunner. If you are interested in building
a streaming Klio job then checkout the :ref:`Klio Streaming Quickstart Guide<hello-klio-streaming>`.

.. attention::

    Be sure to follow the :doc:`installation instructions <installation>` before continuing on.


Create a New Klio Batch Job
---------------------------

:violetemph:`First`, initialize the ``klio_quickstart`` project directory for ``git``:

.. code-block::

    $ git init

.. caution::

    If the |klio-cli|_ was installed via :ref:`option 2 <install-option-2>` or :ref:`option 3 <install-option-3>`, make sure you're ``klio-cli`` virtualenv is activated.


:violetemph:`Next`, within your project directory, run the following command:

.. code-block:: sh

    $ klio job create \
      --job-name klio-quick-start \
      --job-type batch \
      --create-resources \
      --use-defaults


After responding to the prompts, Klio will:

1. Create a GCS bucket in the provided GCP project you provided in the prompt for output data: ``gs://$GCP_PROJECT-output``.
2. Create a `Google Stackdriver`_ dashboard in the provided GCP project for you to monitor runtime job metrics.
3. Create required files within the current working directory.

:violetemph:`Then`, commit the created job files into ``git``:

.. code-block::

    $ git add .
    $ git commit -m "Initial commit for Klio quickstart example"


Run the New Klio Job
--------------------

.. caution::

    If the |klio-cli|_ was installed via :ref:`option 2 <install-option-2>` or :ref:`option 3 <install-option-3>`, make sure you're ``klio-cli`` virtualenv is activated.

:violetemph:`First`, add a file of IDs elements as text input.

The default event input points to a text file in ``klio-quick-start_input_elements.txt`` in a GCS bucket.
To get testing quick this file event input should be replaced with a local file.
A local file containing lines of text corresponding to elements can be created manually
in the top level job directory for this quickstart. Assuming a linux machine, a local file can
also be created by running the following command while in the klio job directory.

.. code-block::

    $ { echo hello
        echo world
      } > klio-quick-start_input_elements.txt

This will create a local file with two lines of text that will serve as the event inputs
of the batch Klio job.

:violetemph:`Then`, run the job using DirectRunner:

.. code-block:: sh

    $ klio job run --direct-runner


Klio will first build a Docker image of the example job with the required dependencies, then start the job locally.
To know it started successfully, you should see a log line containing

.. code-block:: text

    Running pipeline with DirectRunner


Klio will then read from the event input, in this case, the text file that was created.
A :ref:`Klio message<klio-message>` is created for each line in the file
and passed to the ``HelloKlio`` transform.
When the message was successfully consumed, you should see log lines for each KlioMessage.

.. code-block:: text

    Received 'hello' from file './klio-quick-start_input_elements.txt'
    Received 'world' from file './klio-quick-start_input_elements.txt'

.. todo::

    Continue this example on adding (audio) data to read, running on dataflow, etc.
    Add event input to GCS or BigQuery etc.


.. _Google Stackdriver: https://cloud.google.com/stackdriver/docs


.. |klio-cli| replace:: ``klio-cli``
.. _klio-cli: https://pypi.org/project/klio-cli

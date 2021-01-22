.. _migrate-from-setup:

How Do I Migrate from ``setup.py`` to FnAPI?
============================================


The FnAPI (pronounced "fun API") is what allows Klio to use custom Docker images
on Dataflow workers.
However, it's still considered experimental.
The fully-supported way to run a job that has dependencies
(both Python and OS-level dependencies) is via `setup.py <https://beam.apache.org/documentation/
sdks/python-pipeline-dependencies>`_.

Below describes what changes need to be made to an existing job to move from
``setup.py`` to FnAPI.

Creating a new Klio job that does not use ``setup.py`` from the start via:

.. code-block:: console

   $ klio job create --use-fnapi true


Note that this command above will create a job that uses FnAPI for packaging - you will not
need to follow any of the steps below. The steps below convert a job that uses ``setup.py`` for
packaging to one that uses FnAPI.

Required Setup/Changes
----------------------

Update: ``klio-job.yaml``
^^^^^^^^^^^^^^^^^^^^^^^^^

Under ``pipeline_options``:

1. **DELETE** the key ``setup_file``.
2. **ADD** the list key ``experiments`` under ``pipeline_options``, containing the item ``beam_fn_api``. Using the ``beam_fn_api`` experiment in conjunction with setting the ``worker_harness_container_image`` tells Klio and Dataflow to use FnAPI rather than the setup file to package the job.

.. collapsible:: Minimal Example ``klio-job.yaml``

    .. code-block:: yaml

        job_name: my-job
        pipeline_options:
            # NOTE! setup_file is absent
            experiments:
                - beam_fn_api
            worker_harness_container_image: gcr.io/my-project/my-job-image
            runner: DataflowRunner
            # <-- snip -->


Update: ``Dockerfile``
^^^^^^^^^^^^^^^^^^^^^^

Required Changes
~~~~~~~~~~~~~~~~


1. **MOVE** the ``COPY`` line that copies ``job-requirements.txt`` into the image ahead of the rest of the lines that copy in Python files.

2. **UPDATE**  ``RUN pip install .`` to ``RUN pip install -r job-requirements.txt``

.. collapsible:: Why is this needed?

    We now install dependencies directly on the worker image.

3. **MOVE**  ``RUN pip install -r job-requirements.txt`` to the line right after the one from step 1, that copies in ``job-requirements.txt``.

.. collapsible:: Why is this needed?

    This is done as an image build optimization - since your job's Python files are more likely to change than the dependencies in `job-requirements.txt`, it is more efficient install them first.

4. **ADD** any system-level dependencies using ``RUN apt-get update && apt-get install ...``.

.. collapsible:: Why is this needed?

    These dependencies were previously installed through specifying them in ``setup.py`` and running ``pip install .``. They now need to be installed directly on the worker image for your Klio job to use.

.. collapsible:: Example of Required Changes

    .. code-block:: diff

        FROM apache/beam_python3.6_sdk:2.24.0

        WORKDIR /usr/src/app

        ENV GOOGLE_CLOUD_PROJECT my-project \
            PYTHONPATH /usr/src/app

        + RUN apt-get update && apt-get install my-package

        RUN pip install --upgrade pip setuptools

        + COPY job-requirements.txt job-requirements.txt
        + RUN pip install -r job-requirements.txt

        COPY __init__.py \
            run.py \
            transforms.py \
        -   job-requirements.txt \
            /usr/src/app/

        - RUN pip install .

Suggested Changes
~~~~~~~~~~~~~~~~~

The following is a collection of suggested changes to optimize Docker builds by removing no longer used layers and to closer mimic the runtime environment on Dataflow.

.. caution::

    **Most of these changes are incompatible with using setup.py.**

    The following changes will break your job if you return to using ``setup.py`` to package your dependencies. If you choose to switch back, simply undo these deletions.

* **DELETE** lines copying ``MANIFEST.in`` and ``setup.py`` since they are no longer used. If you remove those files from your job directory without also editing your the copy commands out of your Dockerfile, your build will break.

.. collapsible:: Example of Suggested Changes

    .. code-block:: diff

        FROM apache/beam_python3.6_sdk:2.24.0

        WORKDIR /usr/src/app

        ENV GOOGLE_CLOUD_PROJECT my-project \
            PYTHONPATH /usr/src/app

        RUN pip install --upgrade pip setuptools

        COPY __init__.py \
        -   setup.py \
        -   MANIFEST.in \
            klio-job.yaml \
            run.py \
            transforms.py \
            job-requirements.txt \
            /usr/src/app/

        RUN pip install .

.. collapsible:: Combined Example of Required & Suggested Changes

    .. code-block:: diff

        FROM apache/beam_python3.6_sdk:2.24.0

        WORKDIR /usr/src/app

        ENV GOOGLE_CLOUD_PROJECT my-project \
            PYTHONPATH /usr/src/app

        + RUN apt-get update && apt-get install my-package

        RUN pip install --upgrade pip setuptools

        + COPY job-requirements.txt job-requirements.txt
        + RUN pip install -r job-requirements.txt

        COPY __init__.py \
        -   setup.py \
        -   MANIFEST.in \
            klio-job.yaml \
            run.py \
            transforms.py \
        -   job-requirements.txt \
            /usr/src/app/

        -   RUN pip install .

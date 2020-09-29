How Do I Migrate from FnAPI to ``setup.py``?
============================================

The FnAPI (pronounced "fun API") is what allows Klio to run Docker images on Dataflow. However,
it's still considered experimental. The fully-supported way to run a job that has dependencies
(both Python and OS-level dependencies) is via `setup.py <https://beam.apache.org/documentation/
sdks/python-pipeline-dependencies>`_.

Below describes what changes need to be made to an existing job to move from the FnAPI to
``setup.py``.

Creating a new Klio job that does not use the FnAPI from the start via:

.. code-block:: console

    $ klio job create --use-fnapi=false

Limitations and Warnings
------------------------

* Currently, Klio in non-FnAPI mode does not yet support jobs with multiple configuration files. Support is planned.
* ``pipeline_options.requirements_file`` configuration for `pipeline dependencies`_ **will not work** for Klio jobs. While klio will honor that configuration value for Dataflow to pick up, declaring requirements in ``setup.py`` is needed because a Klio job inherently has multiple Python files.
* While Klio will still upload the worker image to `Google Container Registry`_ when running/deploying a job, Dataflow will *not* use the image. It is good practice to upload the worker image to ensure repeatable builds, but in the future, an option will be added to skip the upload.



.. _pipeline dependencies: https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies
.. _Google Container Registry: https://cloud.google.com/container-registry


Required Setup/Changes
----------------------

.. _new-setup-py:

New: ``setup.py``
^^^^^^^^^^^^^^^^^

A ``setup.py`` file is needed in the **root of your job's directory**. It partly substitutes the
need for a worker image by installing any non-Python dependencies via a child process, and by
explicitly including non-Python files needed for a job (i.e. a model, a JSON schema, etc).


.. tip::

    The ``setup.py`` must contain the required system-level dependencies, Python dependencies, and
    required non-Python files (i.e. ML models, JSON schemas, etc) that your job requires to run.

.. collapsible:: Minimal Example ``setup.py``

    The following is an example with a non-Python file (a ML model, ``my-model.h5``), and no
    non-public Python package dependencies or OS-level dependencies.

    .. code-block:: python

        import setuptools

        setuptools.setup(
            name="my-example-job",  # required
            version="0.0.1",  # required
            author="klio-devs",  # optional
            author_email="hello@example.com",  # optional
            description="My example job using setup.py",  # optional
            install_requires=["tensorflow"],  # optional
            data_files=[  # required
                (".", ["klio-job.yaml", "my-model.h5"]),
            ],
            include_package_data=True,  # required
            py_modules=["run", "transforms"],  # required
        )


.. todo:: Show example with internal & OS-level dependencies


New: ``MANIFEST.in``
^^^^^^^^^^^^^^^^^^^^

A file called ``MANIFEST.in`` is needed in the **root of your job's directory** with the line
``include job-requirements.txt``:

.. code-block::

    # cat MANIFEST.in
    include job-requirements.txt


.. collapsible:: Why is this needed?

    The ``MANIFEST.in`` file must include any file required to *install* your job as a Python
    package (but not needed to run your job; those files are declared under ``data_files``
    in setup.py as referred :ref:`above <new-setup-py>`).

    When Klio launches the job for Dataflow, Dataflow will locally create a `source distribution`_
    of your job by running ``python setup.py sdist``. When running this, Python will tar together
    the files declared in ``setup.py`` as well as any non-Python files defined in `MANIFEST.in`_
    into a file called ``workflow.tar.gz`` (as named by Dataflow to then be uploaded).

    Then, on the worker, Dataflow will run ``pip install workflow.tar.gz``. ``pip`` will actually
    build a `wheel`_, installing packages defined in ``job-requirements.txt`` (and running any
    other custom commands defined in ``setup.py``). After the installation of the package via
    ``pip install workflow.tar.gz``, ``job-requirements.txt`` will effectively be gone and
    inaccessible to the job's code. Building a wheel ignores ``MANIFEST.in``, but includes all the
    files declared in ``setup.py``, the ones actually needed for running the Klio job.


Update: Job Logic
^^^^^^^^^^^^^^^^^

Within ``transforms.py`` and any other Python job logic , if there are any references to
non-Python files (i.e. loading a model), the path to those files should be updated to an absolute
path within ``/usr/local``, i.e. ``/usr/local/<filename>``. If your code is having trouble finding
the files, you can try something like:

.. code-block:: python

        import glob, os

        my_model_file = None
        # use iterator so we don't waste time searching everywhere upfront
        files = glob.iglob("/usr/**/my_model.h5", recursive=True)
        for f in files:
            my_model_file = f
            # only grab the first one
            break


.. collapsible:: Why is this needed?

    When using the FnAPI with a provided worker image, the job is just a collection of Python
    modules and itself is not actually installed. With this ``setup.py`` approach, a Python
    package of the Klio job is actually created and installed. And with that, the data files
    declared in ``setup.py`` are installed in ``/usr/local`` on the Dataflow worker.


Update: ``job-requirements.txt``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Within ``job-requirements.txt``, add the package ``klio-exec`` if it's not already there.

.. code-block::

    # cat job-requirements.txt
    klio-exec

.. collapsible:: Why is this needed?

    It's not only required on the worker image to launch the job, Dataflow will need it when it
    unpickles your job code onto the worker. The worker needs access to the exact environment/
    dependencies as the job had when it was launched.


Update: ``klio-job.yaml``
^^^^^^^^^^^^^^^^^^^^^^^^^

Under ``pipeline_options``, add the key ``setup_file`` and set the value to ``setup.py``. This
tells Klio and Dataflow to not use the FnAPI; rather, to use ``setup.py`` as the mechanism for
packaging the job.

.. collapsible:: Minimal Example ``klio-job.yaml``

    .. code-block:: yaml

        job_name: my-job
        pipeline_options:
          setup_file: setup.py # relative to repo root
          worker_harness_container_image: gcr.io/my-project/my-job-image
          runner: DataflowRunner
          # <-- snip -->

.. attention::

    **The worker image is still needed!**

    Do not remove the ``worker_harness_container_image`` value under ``pipeline_options``. Klio
    uses the image as a `"driver"`_ for Beam.


Update: ``Dockerfile``
^^^^^^^^^^^^^^^^^^^^^^

Required Changes
~~~~~~~~~~~~~~~~

1. **ADD** ``klio-job.yaml`` to be copied into ``/usr/src/app``.

    .. collapsible:: Why is this needed?

        We need to include Klio's configuration, but when creating a package of the job, the configuration must be within the same directory ``setup.py`` is in (subdirectories are fine). Relatedly, multi-configuration is not yet supported without the FnAPI since Klio expects the job configuration in a location that we can't manipulate with the ``setup.py`` approach.

2. **ADD** the newly required files to be copied over - ``setup.py`` and ``MANIFEST.in`` - into the working directory, ``/usr/src/app``.

    .. collapsible:: Why is this needed?

        ``setup.py`` and ``MANIFEST.in`` are needed to tell Klio and Dataflow how to build your pipeline as a Python package (i.e. what Python and non-Python files to include) since you're no longer using a Docker image as a "package" for your job.

3. **DOUBLE CHECK** any non-Python files needed for the job, e.g. models, JSON schemas, etc, are copied into the working directory, ``/usr/src/app``.

    .. collapsible:: Why is this needed?

        Klio packages up your job to be installed (for unit tests, audits, and running on the direct runner), and to be uploaded to Dataflow locally on the job's worker image. Therefore, the Docker image needs to have all the required Python and non-Python files to run the job.

4. **ADD** the following line to the end of the file: ``RUN pip install .``

    .. collapsible:: Why is this needed?

        We install the package for the ability to run unit tests via ``klio job test``, run audits via ``klio job audit``, and - if needed - to run the job with Direct Runner.

5. **DOUBLE CHECK** that you ``COPY`` in your ``job-requirements.txt`` file into the image (it should already exist if the job was made via ``klio job create``). It can be grouped into one ``COPY`` line like the example below.

.. collapsible:: Example of Required Changes

    .. code-block:: diff

          COPY __init__.py \
        +     setup.py \
        +     MANIFEST.in \
        +     my-model.h5 \
        +     klio-job.yaml \
        +     job-requirements.txt \
              run.py \
              transforms.py \
              /usr/src/app/

        + RUN pip install .
        # EOF

Suggested Changes
~~~~~~~~~~~~~~~~~

The following is a collection of suggested changes to optimize Docker builds by removing no longer used layers and to closer mimic the runtime environment on Dataflow.

.. caution::

    **Most of these changes are incompatible with the FnAPI.**

    The following changes will break your job if you return to using the FnAPI. If you choose to switch back to the FnAPI, simply undo these deletions.

* **DELETE** any lines updating & installing Debian packages, i.e. ``apt-get update && apt-get install ...``. These commands have been moved to ``setup.py`` and will run via the added line ``RUN pip install .`` .
* **DELETE** any lines referring to ``pip install -r job-requirements.txt`` and ``pip install klio-exec``; dependencies will be installed via the added line ``RUN pip install .``.

    .. note::

        Note: Keeping ``pip install --upgrade pip setuptools`` (or similar) is still advised.

* **DELETE** any lines creating ``/usr/src/config``, i.e. ``RUN mkdir -p /usr/src/config``.
* **DELETE** the two lines ``ARG KLIO_CONFIG=klio-job.yaml`` and ``COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml``.


.. collapsible:: Example of Suggested Changes

    .. code-block:: diff

           FROM apache/beam_python3.6_sdk:2.23.0

           WORKDIR /usr/src/app
        -  RUN mkdir -p /usr/src/config

           ENV GOOGLE_CLOUD_PROJECT my-project \
              PYTHONPATH /usr/src/app

        -  RUN apt-get update && apt-get install -y libsndfile1
        +  RUN pip install --upgrade pip setuptools
        -  RUN pip install --upgrade pip setuptools && \
        -      pip install klio-exec

        -  COPY job-requirements.txt job-requirements.txt
        -  RUN pip install -r job-requirements.txt

           COPY __init__.py \
               run.py \
               transforms.py \
               my-model.h5 \
               /usr/src/app/

        -  ARG KLIO_CONFIG=klio-job.yaml
        -  COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml

.. collapsible:: Combined Example of Required & Suggested Changes

    .. code-block:: diff

           FROM apache/beam_python3.6_sdk:2.23.0

           WORKDIR /usr/src/app
        -  RUN mkdir -p /usr/src/config

           ENV GOOGLE_CLOUD_PROJECT my-project \
              PYTHONPATH /usr/src/app

        -  RUN apt-get update && apt-get install -y libsndfile1
        +  RUN pip install --upgrade pip setuptools
        -  RUN pip install --upgrade pip setuptools && \
        -      pip install klio-exec

        -  COPY job-requirements.txt job-requirements.txt
        -  RUN pip install -r job-requirements.txt

           COPY __init__.py \
        +      setup.py \
        +      MANIFEST.in \
        +      job-requirements.txt \
        +      my-model.h5 \
        +      klio-job.yaml \
               run.py \
               transforms.py \
               /usr/src/app/

        -  ARG KLIO_CONFIG=klio-job.yaml
        -  COPY $KLIO_CONFIG /usr/src/config/.effective-klio-job.yaml
        +  RUN pip install .

.. _source distribution: https://packaging.python.org/guides/distributing-packages-using-setuptools/#source-distributions
.. _MANIFEST.in: https://packaging.python.org/guides/distributing-packages-using-setuptools/#manifest-in
.. _wheel: https://packaging.python.org/guides/distributing-packages-using-setuptools/#wheels
.. _"driver": https://beam.apache.org/documentation/programming-guide/#overview

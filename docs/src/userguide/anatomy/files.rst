Files of a Klio Job
===================

There are a handful of files that are required for a Klio job, which the ``klio job create`` :ref:`command <klio-job-create>` automatically generates for you.


.. code-block:: sh

    $ tree

    .
    ├── Dockerfile
    ├── MANIFEST.in
    ├── README.md
    ├── __init__.py
    ├── job-requirements.txt
    ├── klio-job.yaml
    ├── run.py
    ├── setup.py
    ├── test_transforms.py
    └── transforms.py

    0 directories, 8 files


Python Files
------------

``run.py`` and ``transforms.py`` jointly define the logic of your streaming job (the pink box of "your code" in the :doc:`job overview diagram <job>`).

``test_transforms.py`` gives you a place to test the logic of your streaming job.

``__init__.py`` helps the Python executable find the path where the other Python files are.

``setup.py`` file defines how the job should be packaged so the configured runner can appropriately install it.
This is also where job-specific :violetemph:`system-level dependencies` should be declared.

Dependency Declaration
----------------------

``job-requirements.txt`` is a standard ``pip`` `requirements file`_ where you declare the Python packages needed in your Klio job.

``Dockerfile`` describes a Docker image that will be used for launching a Klio job, regardless of the configured runner. It is setup to install the packages you specify in ``job-requirements.txt``.


Configuration
-------------
``klio-job.yaml`` contains configuration that tells a runner how to run your job.
It also stores Klio job-level information like event and data I/O configuration, metrics, etc.
Read more about a job's configuration :doc:`here <../config/index>`.


Other Files
-----------

``MANIFEST.in`` declares files needed for job installation (i.e. ``job-requirements.txt``) but not needed at runtime.

.. collapsible:: Why is this needed?

    The ``MANIFEST.in`` file must include any file required to *install* your job as a Python
    package (but not needed to run your job; those files are declared under ``data_files``
    in ``setup.py`` as referred above).

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

``README.md`` is initially populated with setup instructions on how to get a job running, but should be used however needed.



.. _requirements file: https://pip.pypa.io/en/stable/user_guide/#requirements-files
.. _source distribution: https://packaging.python.org/guides/distributing-packages-using-setuptools/#source-distributions
.. _MANIFEST.in: https://packaging.python.org/guides/distributing-packages-using-setuptools/#manifest-in
.. _wheel: https://packaging.python.org/guides/distributing-packages-using-setuptools/#wheels

Files of a Klio Job
===================

There are a handful of files that are required for a Klio job, which the ``klio job create`` :ref:`command <klio-job-create>` automatically generates for you.


.. code-block:: sh

    $ tree

    .
    ├── Dockerfile
    ├── MANIFEST.in          # for non-FnAPI jobs
    ├── README.md
    ├── __init__.py
    ├── job-requirements.txt
    ├── klio-job.yaml
    ├── run.py
    ├── setup.py             # for non-FnAPI jobs
    ├── test_transforms.py
    └── transforms.py

    0 directories, 8 files


Python Files
------------

``run.py`` and ``transforms.py`` jointly define the logic of your streaming job (the pink box of "your code" in the :doc:`job overview diagram <job>`).

``test_transforms.py`` gives you a place to test the logic of your streaming job.

``__init__.py`` helps the Python executable find the path where the other Python files are.

For :greenemph:`non-FnAPI jobs`, a ``setup.py`` file is generated to define how the job should be packaged so the configured runner can appropriately install it.
This is also where job-specific :greenemph:`system-level dependencies` should be declared when the FnAPI is not used.
See :ref:`new-setup-py` for more information.

Dependency Declaration
----------------------

``job-requirements.txt`` is a standard ``pip`` `requirements file`_ where you declare the Python packages needed in your Klio job.

``Dockerfile`` describes a Docker image that will be used for launching a Klio job, regardless of the configured runner. It is setup to install the packages you specify in ``job-requirements.txt``.


For :greenemph:`FnAPI jobs`, it is also where job-specific :greenemph:`system-level dependencies` should be declared (i.e. ``apt-get install ffmpeg``) and whatever else required to setup a job's environment.

Configuration
-------------
``klio-job.yaml`` contains configuration that tells a runner how to run your job.
It also stores Klio job-level information like event and data I/O configuration, metrics, etc.
Read more about a job's configuration :doc:`here <../config/index>`.


Other Files
-----------

For :greenemph:`non-FnAPI jobs`, a ``MANIFEST.in`` is generated that declares files needed for job installation (i.e. ``job-requirements.txt``) but not needed at runtime.
See :ref:`new-manifest-in` for more information.

``README.md`` is initially populated with setup instructions on how to get a job running, but should be used however needed.



.. _requirements file: https://pip.pypa.io/en/stable/user_guide/#requirements-files

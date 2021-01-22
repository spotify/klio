What do I need to know about using ``setup.py`` to package dependencies in Klio?
================================================================================

Using ``setup.py`` file to specify dependencies to install on workers
was the original means by which a Python Beam job developer could
customize the environment in which their job ran.
You can read more about it
`here <https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/>`_.

To configure a Klio job to run using ``setup.py`` packaging,
the user needs to provide the required files (see below)
and configure the job to use them
by filling in the ``pipeline_options.setup_file`` field in their ``klio-job.yaml`` file:


.. code-block:: yaml

    job_name: my-job
    pipeline_options:
        setup_file: setup.py
        # <-- snip -->

Note that this method of setup is mutually exclusive with :ref:`using the FnAPI <migrate-from-fnapi>` for packaging.

Required Files
--------------

.. _setup-py:

``setup.py``
^^^^^^^^^^^^

A ``setup.py`` file is needed in the **root of your job's directory**. It partly substitutes the
need for a worker image by installing any non-Python dependencies via a child process, and by
explicitly including non-Python files needed for a job (i.e. a model, a JSON schema, etc).


.. tip::

    The ``setup.py`` must contain the required system-level dependencies, Python dependencies, and
    required non-Python files (i.e. ML models, JSON schemas, etc) that your job requires to run.

.. collapsible:: Minimal Example ``setup.py``

    The following is an example with
    a third party Python dependency available on PyPI
    (but no non-public Python package dependencies),
    a non-Python file (an ML model, ``my-model.h5``),
    and no OS-level dependencies.

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



.. collapsible:: Example ``setup.py`` with internal & OS-level dependencies

    The following is a minimal example
    that shows a method for including internal & OS-level dependencies, in
    addition to .

    .. code-block:: python

        import subprocess
        import setuptools

        from distutils.command.build import build as _build


        # NOTE: This class is required when using custom commands (i.e.
        # installing OS-level deps and/or deps from internal PyPI)
        class build(_build):
            """A build command class that will be invoked during package install.

            The package built using the current setup.py will be staged and
            later installed in the worker using `pip install package'. This
            class will be instantiated during install for this specific scenario
            and will trigger running the custom commands specified.
            """
            sub_commands = _build.sub_commands + [('CustomCommands', None)]

        # `APT_COMMANDS` and `REQUIREMENTS_COMMANDS` are custom commands that
        # will run during setup that are required for this Klio job. Each
        # command will spawn a child process.
        APT_COMMANDS = [
            ["apt-get", "update"],
            # Debian-packaged dependencies (or otherwise OS-level requirements)
            # `--assume-yes` to avoid interactive confirmation
            ["apt-get", "install", "--assume-yes", "libsndfile1"],
        ]
        REQUIREMENTS_COMMANDS = [
            [
                "pip3",
                "install",
                "--default-timeout=120",
                # --index-url will not work on Dataflow, but `--extra-index-url` will
                "--extra-index-url",
                # point to your internal PyPI
                "pypi.internal.net",
                "--requirement",
                "job-requirements.txt",  # Must also be included in MANIFEST.in
            ]
        ]


        # NOTE: This class is required when using custom commands (i.e.
        # installing OS-level deps and/or deps from internal PyPI)
        class CustomCommands(setuptools.Command):
            """A setuptools Command class able to run arbitrary commands."""

            def initialize_options(self):
                # Method must be defined when implementing custom Commands
                pass

            def finalize_options(self):
                # Method must be defined when implementing custom Commands
                pass

            def RunCustomCommand(self, command_list):
                # NOTE: Output from the custom commands are missing from the
                # logs. The output of custom commands (including failures) will
                # be logged in the worker-startup log. (BEAM-3237)
                print("Running command: %s" % " ".join(command_list))
                p = subprocess.Popen(
                    command_list,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                stdout_data, _ = p.communicate()
                print("Command output: %s" % stdout_data)
                if p.returncode != 0:
                    raise RuntimeError(
                        "Command {} failed: exit code: {}".format(
                            command_list, p.returncode
                        )
                    )

            def run(self):
                for command in APT_COMMANDS:
                    self.RunCustomCommand(command)
                for command in REQUIREMENTS_COMMANDS:
                    self.RunCustomCommand(command)


        # NOTE: `version` does not particularly mean anything here since we're
        # not publishing to PyPI; it's just a required field
        setuptools.setup(
            name="my-example-job",  # required
            version="0.0.1",  # required
            author="klio-devs",  # optional
            author_email="hello@example.com",  # optional
            description="My example job using setup.py",  # optional
            install_requires=[],  # optional if using the above REQUIREMENTS_COMMANDS
            data_files=[  # required
                # tuple(
                #    str(dir where to install files, relative to Python modules),
                #    list(str(non-Python filenames))
                # )
                (".", ["klio-job.yaml", "my-model.h5"]),
            ],
            include_package_data=True,  # required
            py_modules=["run", "transforms"],  # required
            # NOTE: required when using custom commands (i.e. installing OS-level
            # deps and/or deps from internal PyPI)
            cmdclass={  # optional
                # Command class instantiated and run during pip install scenarios.
                "build": build,
                "CustomCommands": CustomCommands,
            },
        )

.. _manifest-in:

``MANIFEST.in``
^^^^^^^^^^^^^^^

A file called ``MANIFEST.in`` is needed in the **root of your job's directory** with the line
``include job-requirements.txt``:

.. code-block::

    # cat MANIFEST.in
    include job-requirements.txt


.. collapsible:: Why is this needed?

    The ``MANIFEST.in`` file must include any file required to *install* your job as a Python
    package (but not needed to run your job; those files are declared under ``data_files``
    in setup.py as referred :ref:`above <setup-py>`).

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




Limitations and Warnings
------------------------

* Currently, Klio in non-FnAPI mode does not yet support jobs with multiple configuration files. Support is planned.
* ``pipeline_options.requirements_file`` configuration for `pipeline dependencies`_ **will not work** for Klio jobs. While klio will honor that configuration value for Dataflow to pick up, declaring requirements in ``setup.py`` is needed because a Klio job inherently has multiple Python files.
* While Klio will still upload the worker image to `Google Container Registry`_ when running/deploying a job, Dataflow will *not* use the image. It is good practice to upload the worker image to ensure repeatable builds, but in the future, an option will be added to skip the upload.

.. _source distribution: https://packaging.python.org/guides/distributing-packages-using-setuptools/#source-distributions
.. _MANIFEST.in: https://packaging.python.org/guides/distributing-packages-using-setuptools/#manifest-in
.. _wheel: https://packaging.python.org/guides/distributing-packages-using-setuptools/#wheels
.. _pipeline dependencies: https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#pypi-dependencies
.. _Google Container Registry: https://cloud.google.com/container-registry
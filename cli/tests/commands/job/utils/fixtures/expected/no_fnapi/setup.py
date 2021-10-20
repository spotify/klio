"""Setup.py module for the workflow's worker utilities.

All the Klio job-related code is gathered in a package that will be
built as a source distribution (via `python setup.py sdist`, staged in
the staging area (as defined in `klio-job.yaml` under `pipeline_options.
staging_location`) for the job being run and then installed in the
Dataflow workers when they start running.

This behavior is triggered by specifying the `setup_file` value under
`pipeline_options` in `klio-job.yaml` when running a Klio job.

This approach is adapted from
  https://github.com/apache/beam/blob/master/sdks/python/apache_beam/
          examples/complete/juliaset/setup.py.
"""

import subprocess

from distutils.command.build import build as _build

import setuptools


# NOTE: This class is required when using custom commands (i.e.
# installing OS-level deps and/or deps from a private PyPI)
class build(_build):
    """A build command class that will be invoked during package install.

    The package built using the current setup.py will be staged and
    later installed in the worker using `pip install package'. This
    class will be instantiated during install for this specific scenario
    and will trigger running the custom commands specified.
    """

    sub_commands = _build.sub_commands + [("CustomCommands", None)]


# `APT_COMMANDS` and `REQUIREMENTS_COMMANDS` are custom commands that
# will run during setup that are required for this Klio job. Each
# command will spawn a child process.

APT_COMMANDS = [
#     # Example of defining OS-level depedencies
#     ["apt-get", "update"],
#     # Debian-packaged dependencies (or otherwise OS-level requirements)
#     # `--assume-yes` to avoid interactive confirmation
#     ["apt-get", "install", "--assume-yes", "<MY_DEP_1>", "<MY_DEP_2>"],
]


# NOTE: installing dependencies through `install_requires` in the `setup`
# function below **does not  work** when needing to access a private
# PyPI (hence the REQUIREMENTS_COMMANDS below), even with
# `dependency_links` set.
# NOTE: any Python dependency that depends on a system-level package
# (i.e. installed via an `apt` command above) should be here and **not**
# passed in via `install_requires` in the `setup` function below because
# dependencies will be installed **before** the APT_COMMANDS are run.
# NOTE: if using `job-requirements.txt` to list dependencies (rather
# than hard-coding them here) then the filename must be included in a
# `MANIFEST.in` file so that it is included when uploaded to Dataflow.
REQUIREMENTS_COMMANDS = [
    [
        "pip3",
        "install",
        "--default-timeout=120",
        "--requirement",
        "job-requirements.txt",  # Must also be included in MANIFEST.in
    ]
]


# NOTE: This class is required when using custom commands (i.e.
# installing OS-level deps and/or deps from a private PyPI)
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
    name="test-job",  # required
    version="0.0.1",  # required
    # NOTE: required only when *not* using the above REQUIREMENTS_COMMANDS
    # approach to install deps (i.e. deps that don't require OS-level deps
    # and/or not from a private PyPI)
    install_requires=[],  # optional
    # NOTE: Any other non-Python files required to run a Klio job *must* be
    # listed in here in `data_files`
    data_files=[  # required
        # tuple(
        #    str(dir where to install files, relative to Python modules),
        #    list(str(non-Python filenames))
        # )
    ],
    include_package_data=True,  # required
    # NOTE: Explicitly include Python modules names (all relevant Python files
    # needed for running a job, without the .py extension)
    py_modules=["run", "transforms"],  # required
    # NOTE: required when using custom commands (i.e. installing OS-level
    # deps and/or deps from a private PyPI)
    cmdclass={  # optional
        # Command class instantiated and run during pip install scenarios.
        "build": build,
        "CustomCommands": CustomCommands,
    },
)

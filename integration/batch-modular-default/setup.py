#! /usr/bin/env python
#
# Copyright 2020 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
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
          examples/complete/juliaset/setup.py

Please see `README.md` in this directory for more information.
"""

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
    # ["apt-get", "update"],
    # Debian-packaged dependencies (or otherwise OS-level requirements)
    # `--assume-yes` to avoid interactive confirmation
    # ["apt-get", "install", "--assume-yes", "libsndfile1"],
]
# NOTE: installing dependencies through `install_requires` in the `setup`
# function below **does not  work** when needing to access our internal
# PyPI (hence the REQUIREMENTS_COMMANDS below), even with
# `dependency_links` set.
# NOTE: any Python dependency that depends on a system-level package
# (i.e. installed via an `apt` command above) should be here and **not**
# passed in via `install_requires` in the `setup` function below because
# dependencies will be installed **before** the APT_COMMANDS are run.
# NOTE: if using `job-requirements.txt` to list dependencies (rather
# than hard-coding them here) then the filename must be included in a
# `MANIFEST.in` file (like it is in this example) so that it is included
# when uploaded to Dataflow.
REQUIREMENTS_COMMANDS = [
    [
        "pip3",
        "install",
        "--default-timeout=120",
        # --index-url will not work on Dataflow, but `--extra-index-url` will
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
    name="batch-modular-default",  # required
    version="0.0.1",  # required
    author="The klio devs",  # optional
    author_email="opensource+klio@spotify.com",  # optional
    description="Batch modular default test",  # optional
    url="https://github.com/klio/integration/batch-modular-default",  # optional
    # NOTE: required only when *not* using the above REQUIREMENTS_COMMANDS
    # approach to install deps (i.e. deps that don't require OS-level deps
    # and/or not from internal PyPI)
    install_requires=[],  # optional
    # NOTE: The `klio-job.yaml` and any other non-Python files
    # required to run a Klio job *must* be listed in here in `data_files`
    data_files=[  # required
        # tuple(
        #    str(dir where to install files, relative to Python modules),
        #    list(str(non-Python filenames))
        # )
        (".", ["klio-job-run-effective.yaml"]),
    ],
    include_package_data=True,  # required
    # NOTE: Explicitly include Python modules names (all relevant Python files
    # needed for running a job, without the .py extension)
    py_modules=["run", "transforms"],  # required
    # NOTE: required when using custom commands (i.e. installing OS-level
    # deps and/or deps from internal PyPI)
    cmdclass={  # optional
        # Command class instantiated and run during pip install scenarios.
        "build": build,
        "CustomCommands": CustomCommands,
    },
)

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

import platform

from packaging import version

from klio_exec.commands.audit_steps import base


class NumPyBrokenBLASUsage(base.BaseKlioAuditStep):
    """Detect numpy version for potential threading issues."""

    AUDIT_STEP_NAME = "numpy_broken_blas"
    MINIMUM_NUMPY_VERSION = version.parse("1.16.3")

    @staticmethod
    def get_description():
        # tmp turn off black formatting to skip long URL
        # fmt: off
        return (
            "The 1.16.3 version of numpy links against a newer version of "
            "OpenBLAS that fixes some important threading issues - notably, "
            "the `dot` function that calls into OpenBLAS' _dgemv function, "
            "which on older versions, is non-reentrant and can cause both "
            "incorrect results and deadlocks.\n\n"
            "See:\n"
            "\t- https://github.com/numpy/numpy/blob/2f70544179e24b0ebc0263111f36e6450bbccf94/doc/source/release/1.16.3-notes.rst#numpy-1163-release-notes\n"  # noqa: E501
            "\t- https://github.com/xianyi/OpenBLAS/issues/1844\n"
            "\t- https://github.com/numpy/numpy/issues/12394\n"
            "\t- https://github.com/xianyi/OpenBLAS/pull/1865\n"
        )
        # fmt: on

    @property
    def _is_job_single_threaded_per_container(self):
        exps = self.klio_config.pipeline_options.experiments
        return "worker_threads=1" in exps

    @staticmethod
    def _get_current_numpy_version():
        try:
            import numpy

            return version.parse(numpy.version.short_version)
        except ImportError:
            return None

    def after_tests(self):
        if self._is_job_single_threaded_per_container:
            return

        if platform.system().lower() != "linux":
            return

        numpy_version = self._get_current_numpy_version()
        if numpy_version is None:
            return

        if numpy_version < NumPyBrokenBLASUsage.MINIMUM_NUMPY_VERSION:
            msg = (
                "Multiple threads are used, but a NumPy version older than %s "
                "was detected. Older versions of NumPy are known to be "
                "thread-unsafe due to a broken OpenBLAS dependency on Linux."
            ) % NumPyBrokenBLASUsage.MINIMUM_NUMPY_VERSION
            self.emit_error(msg)


_init = NumPyBrokenBLASUsage

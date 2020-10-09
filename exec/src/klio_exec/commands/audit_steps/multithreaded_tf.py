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

import sys

from klio_exec.commands.audit_steps import base


class MultithreadedTFUsage(base.BaseKlioAuditStep):
    """Use caution when running tensorflow in a multithreaded environment."""

    AUDIT_STEP_NAME = "multithreaded_tf"

    @property
    def _is_tensorflow_loaded(self):
        return any(["tensorflow" in module for module in sys.modules])

    @property
    def _is_job_single_threaded_per_container(self):
        exps = self.klio_config.pipeline_options.experiments
        return "worker_threads=1" in exps

    def after_tests(self):
        if not self._is_job_single_threaded_per_container:
            if self._is_tensorflow_loaded:
                self.emit_warning(
                    "TensorFlow usage detected within job, but "
                    "`worker_threads` is not explicitly set to 1 under "
                    "`pipeline_options.experiments` in the job's configuration "
                    "file! This can cause threading issues. Be careful."
                )


_init = MultithreadedTFUsage

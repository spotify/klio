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

import tempfile
import traceback

from klio_exec.commands.audit_steps import base


class TempFileUsage(base.BaseKlioAuditStep):
    """Avoid leaky file descriptors from `tempfile.TemporaryFile`."""

    AUDIT_STEP_NAME = "tempfile"
    PACKAGES_TO_IGNORE = ("_pytest",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tempfile_used = False
        self._tempfile_tracebacks = []

    def _mock_tempfile(self):
        """Override tempfile.TemporaryFile in the user's code
            with a MockTemporaryFile that marks the class attribute
            `TempfileUsage.AuditStep.mock_temporary_file_was used`
             as True before returning an actual tempfile.TemporaryFile.

            Ignores any use of tempfile.TemporaryFile by
            pytest.
        """
        RealTempFile = tempfile.TemporaryFile

        def MockTemporaryFile(*args, **kwargs):
            stack = traceback.extract_stack()[:-1]

            caller_frame = stack[-1]
            should_ignore = any(
                [
                    ("/%s/" % ignored) in caller_frame.filename
                    for ignored in TempFileUsage.PACKAGES_TO_IGNORE
                ]
            )
            if not should_ignore:
                self._tempfile_used = True
                self._tempfile_tracebacks.append(stack)
            return RealTempFile(*args, **kwargs)

        tempfile.TemporaryFile = MockTemporaryFile

    def before_tests(self):
        self._mock_tempfile()

    def after_tests(self):
        if self._tempfile_used:
            self.emit_error(
                "`tempfile.TemporaryFile` was used! Please use `tempfile."
                "NamedTemporaryFile` instead to avoid leaking files. "
                "Traceback:",
                self._tempfile_tracebacks[0],
            )


# shortcut for registering plugins in setup.py
_init = TempFileUsage

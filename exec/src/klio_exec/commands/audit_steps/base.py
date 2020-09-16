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

import traceback


def _get_relevant_frames(tb):
    """Remove up to and including pytest-related frames from tb.

    Args:
        list(traceback.FrameSummary): complete traceback of code
            executed by pytest
    Returns:
        list(traceback.FrameSummary) of traceback frames not including
            the invocation of pytest
    """
    last_index = 0
    for i, frame in enumerate(reversed(tb)):
        if "pytest" in str(frame):
            last_index = len(tb) - i
            break
    return tb[last_index:]


class BaseKlioAuditStep(object):
    """
    A base class to be subclassed when creating new audit steps for
    auditing Klio jobs.

    Subclasses have access to the following properties:
     - self.job_dir
     - self.klio_config

    All other inspection or modification of the Python environment
    should happen using global state, as pytest itself is run in between
    before_tests and after_tests.
    """

    AUDIT_STEP_NAME = None

    def __init__(self, job_dir, klio_config, tw):
        self.klio_config = klio_config
        self.job_dir = job_dir
        self.tw = tw
        self.errored = False
        self.warned = False

    @staticmethod
    def get_description():
        """
        User-facing description of audit step shown in `klio job audit --list`.

        If not implemented, the audit step class's docstring is used.
        If there is no class-level docstring, description defaults to
        "No description".
        """
        pass

    def _emit(self, message, tb=None, **kw):
        msg = "[{}]: {}\n".format(self.AUDIT_STEP_NAME, message)
        if tb:
            tb = _get_relevant_frames(tb)
            tb_fmtd = "\n".join(traceback.format_list(tb))
            msg = "{}{}\n".format(msg, tb_fmtd)

        self.tw.write(msg, **kw)

    def emit_warning(self, warning, tb=None, **kw):
        """Emit audit warning and set `self.warned` to True.

        Excludes any pytest related stacktraces in tb.

        Args:
              warning(str): Warning to emit. Prefixed to tb.
              tb(traceback object): Traceback to emit.
              kw(dict): Any attributes to pass onto `TerminalWriter.write`.
        """
        self.warned = True

        kwargs = {"yellow": True}
        kwargs.update(kw)
        self._emit(warning, tb=tb, **kwargs)

    def emit_error(self, error, tb=None, **kw):
        """Emit audit error and set `self.errored` to True.

        Excludes any pytest related stacktraces in tb.

        Args:
              warning(str): Warning to emit. Prefixed to tb.
              tb(traceback object): Traceback to emit.
              kw(dict): Any attributes to pass onto `TerminalWriter.write`.
        """
        self.errored = True

        kwargs = {"red": True}
        kwargs.update(kw)
        self._emit(error, tb=tb, **kwargs)

    def before_tests(self):
        """Any setup needed for test."""
        pass

    def after_tests(self):
        """Evaluate the results of auditing and emit results.

        Users can use the `emit_warning` and `emit_error` methods
        here to simplify reporting results. Implementation required.
        """
        raise NotImplementedError("Subclasses must implement after_tests!")

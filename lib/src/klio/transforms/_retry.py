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
"""
This module is inspired by https://github.com/invl/retry
and https://github.com/pnpnpn/retry-decorator.
It's been simplified for what we need, and adapted to work with generators.
"""

import logging
import threading
import types


TRACEBACK_HEADER = "Traceback (most recent call last):\n"


class KlioRetriesExhausted(Exception):
    """No more retries left."""


class KlioRetryWrapper(object):
    """Wrap a function with configured retries."""

    DEFAULT_EXC_MSG = (
        "Function '{}' has reached the maximum {} retries. Last exception: {}"
    )

    def __init__(
        self,
        function,
        tries,
        delay=None,
        exception=None,
        raise_exception=None,
        exception_message=None,
    ):
        self._function = function
        self._func_name = getattr(function, "__qualname__", function.__name__)
        self._tries = tries
        self._delay = delay or 0.0
        self._exception = exception or Exception
        self._retry_exception = raise_exception or KlioRetriesExhausted
        self._exception_message = exception_message
        self._logger = logging.getLogger("klio")

    def __call__(self, *args, **kwargs):
        tries = self._tries

        while tries:
            try:
                ret = self._function(*args, **kwargs)
                if isinstance(ret, types.GeneratorType):
                    ret = next(ret)
                return ret

            except self._exception as e:
                tries -= 1
                if not tries:
                    self._raise_exception(e)
                    break

                msg = self._format_log_message(tries, e)
                self._logger.warning(msg)

                if not self._delay > 0.0:
                    continue

                # use a Condition to avoid using `time.sleep` as that will
                # block other threads; we just want to block this thread
                condition = threading.Condition()
                condition.acquire()
                condition.wait(self._delay)
                condition.release()

    def _format_log_message(self, tries, exception):
        msg_prefix = "Retrying KlioMessage"
        if self._delay > 0:
            msg_prefix = "{} in {} seconds".format(msg_prefix, self._delay)

        attempts = self._tries - tries
        msg_attempts = "attempt {}".format(attempts)
        if self._tries > 0:
            msg_attempts = "{} of {}".format(msg_attempts, self._tries)

        msg = "{} - {}. '{}' raised an exception: {} ".format(
            msg_prefix, msg_attempts, self._func_name, exception
        )

        return msg

    def _raise_exception(self, last_exception):
        if self._exception_message is None:
            self._exception_message = self.DEFAULT_EXC_MSG.format(
                self._func_name, self._tries, last_exception
            )
        error = self._retry_exception(self._exception_message)
        self._logger.error(
            "Dropping KlioMessage - retries exhausted: {}".format(error)
        )
        raise error

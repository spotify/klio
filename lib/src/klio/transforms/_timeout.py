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
This module is heavily inspired by https://github.com/pnpnpn/timeout-decorator
and https://github.com/bitranox/wrapt_timeout_decorator.
It's been simplified for what we need, and adapted to work with generators.
"""

import multiprocessing
import sys
import time
import traceback
import types

import psutil


TRACEBACK_HEADER = "Traceback (most recent call last):\n"


class KlioTimeoutError(Exception):
    """Transform timed out while processing a KlioMessage."""


def _target(queue, function, *args, **kwargs):
    """Run user's function with args/kwargs and return output via a queue."""
    try:
        ret = function(*args, **kwargs)
        if isinstance(ret, types.GeneratorType):
            ret = next(ret)
        queue.put((True, ret))

    except Exception:
        # Note: here we're trying to get the traceback context that's relevant
        # to the user. Without it, the user would see the timeout wrapper
        # context, which won't help the user. So we grab the previous traceback
        # (accessed by tb_next) and attach it to the exception as a new
        # attribute. Then when we catch the exception in the caller code, we
        # can check for that attribute and log it.
        exc_infos = sys.exc_info()
        user_exc = exc_infos[1]
        user_exc_tb = "".join(traceback.format_tb(exc_infos[2].tb_next))
        user_exc_tb = TRACEBACK_HEADER + user_exc_tb
        setattr(user_exc, "_klio_traceback", user_exc_tb)
        queue.put((False, user_exc))


class KlioTimeoutWrapper(object):
    """Wrap a function to execute in a separate process with a timeout."""

    DEFAULT_EXC_MSG = "Function '{}' timed out after {} seconds."

    def __init__(
        self, function, seconds, timeout_exception=None, exception_message=None
    ):
        self._function = function
        self._func_name = getattr(function, "__qualname__", function.__name__)
        self._seconds = seconds
        self._timeout_exception = timeout_exception or KlioTimeoutError
        self._exception_message = exception_message

    def __call__(self, *args, **kwargs):
        self._queue = multiprocessing.Queue(maxsize=1)
        args = (self._queue, self._function) + args
        proc_name = "KlioTimeoutProcess-{}".format(self._func_name)
        self._process = multiprocessing.Process(
            target=_target, name=proc_name, args=args, kwargs=kwargs
        )
        # can't daemonize process in case users nest decorators
        self._process.daemon = False
        self._process.start()
        self._timeout = self._seconds + time.monotonic()

        while not self.ready:
            time.sleep(0.01)
        return self.value

    def _raise_exception(self):
        if self._exception_message is None:
            self._exception_message = self.DEFAULT_EXC_MSG.format(
                self._func_name, self._seconds
            )
        raise self._timeout_exception(self._exception_message)

    def _terminate_child_processes(self):
        # This is to make sure we clean up any spawned child processes,
        # particularly if the user uses `subprocess` in their timeout-
        # decorated code
        proc = psutil.Process(pid=self._process.pid)
        children = proc.children(recursive=True)
        while children:
            # top-down/handle parents first; if we terminate children first,
            # the parents could just respawn
            child = children.pop(0)
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass

    def cancel(self):
        """Terminate any possible execution of the process running the func."""
        if self._process.is_alive():
            self._terminate_child_processes()
            self._process.terminate()

        # join process so the parent process can reap it - no zombies
        self._process.join(timeout=0.1)
        self._raise_exception()

    @property
    def ready(self):
        """Manage the status of "value" property."""
        if self._timeout < time.monotonic():
            self.cancel()
        return self._queue.full() and not self._queue.empty()

    @property
    def value(self):
        """Get data from the processing queue that was added by the func."""
        if self.ready is True:
            is_successful, ret_value = self._queue.get()
            if is_successful:
                return ret_value
            raise ret_value

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

import enum
import logging
import multiprocessing
import threading


class _DummySemaphore(object):
    """Mock semaphore for API parity when no limit is set in ThreadLimiter."""

    def __init__(self, *args, **kwargs):
        self._value = "NONE"

    def acquire(self):
        pass

    def release(self):
        pass


class ThreadLimit(enum.Enum):
    """:class:`enum.Enum` of constants for :class:`ThreadLimiter`.

    Example usage:

    .. code-block:: python

        from klio import utils

        thread_limiter = utils.ThreadLimiter(
            max_thread_limit=utils.ThreadLimit.DEFAULT
        )
    """

    NONE = 0
    """Do not limit the number of threads used."""
    DEFAULT = 1
    """Default thread limit (CPU count of worker machine via
    :func:`multiprocessing.cpu_count`).
    """


class ThreadLimiter(object):
    """A context manager to limit active threads for a block of code.

    Threads aren't limited `directly`; threads are limited indirectly
    by maintaining a semaphore (an atomic counter).

    Example usage:

    .. code-block:: python

        # No arg to use the default limit (CPU count of host/worker):
        thread_limiter = ThreadLimiter()

        # Then use it as a context manager:
        with thread_limiter:
            ...

        # Or explicitly call acquire and release:
        thread_limiter.acquire()
        ...
        thread_limiter.release()

        # Explicitly use the default limit:
        thread_limiter = ThreadLimiter(max_thread_count=ThreadLimit.DEFAULT)

        # Set your own limit:
        thread_limiter = ThreadLimiter(max_thread_count=2)

        # Dynamically set the limit using a function
        limit_func = lambda: multiprocessing.cpu_count() * 4
        thread_limiter = ThreadLimiter(max_thread_count=limit_func)

        # Turn off thread limiting
        thread_limiter = ThreadLimiter(max_thread_count=ThreadLimit.NONE)

    Args:
        max_thread_count (int, callable, ThreadLimit): number of threads
            to make available to the limiter, or a :func:`callable` that
            returns an ``int``. Values must be greater or equal to 0.
            Set to :attr:`ThreadLimit.NONE` for no thread limits.
        name (str): Name of particular limiter. Defaults to object ID via
            ``id(self)``.
    """

    _PREFIX = "KlioThreadLimiter"

    def __init__(self, max_thread_count=ThreadLimit.DEFAULT, name=None):
        self.__repr_name = f"name={name}" if name else f"id={id(self)}"
        self.logger = logging.getLogger("klio.concurrency")

        if max_thread_count is ThreadLimit.DEFAULT:
            max_thread_count = multiprocessing.cpu_count

        if max_thread_count is ThreadLimit.NONE:
            self._dummy = True
            self._semaphore = _DummySemaphore()
            self.logger.debug(f"{self} Using unlimited semaphore")

        else:
            self._dummy = False
            if callable(max_thread_count):
                max_thread_count = max_thread_count()
            self._semaphore = threading.BoundedSemaphore(max_thread_count)
            self.logger.debug(
                f"{self} Initial semaphore value: {self._semaphore._value}"
            )

    def _log(self, message):
        threads_available = False
        suffix_msg = ""
        if not self._dummy:
            threads_available = self._semaphore._value
            suffix_msg = f" (available threads: {threads_available})"
        self.logger.debug(f"{self} {message}{suffix_msg}")

    def acquire(self):
        """Acquire a semaphore (a thread).

        Acquiring a semaphore will activate an available thread in Beam's
        multi-threaded environment.

        If no semaphores are available, the method will block until one
        is released via :func:`ThreadLimiter.release`.
        """
        self._log("Blocked – waiting on semaphore for an available thread")
        self._semaphore.acquire()

    def release(self):
        """Release a semaphore (a thread).

        Raises:
            `ValueError`: if the semaphore is released too many times (more
                than the value of ``ThreadLimiter._semaphore._value``).
        """
        self._semaphore.release()
        self._log("Released semaphore")

    def __repr__(self):
        return f"{self._PREFIX}({self.__repr_name})"

    def __enter__(self):
        self.acquire()
        return self._semaphore

    def __exit__(self, type, value, traceback):
        self.release()

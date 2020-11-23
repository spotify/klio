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

import logging
import multiprocessing
import threading


class _DummySemaphore(object):
    """TODO"""

    def __init__(self, *args, **kwargs):
        self._value = "NONE"

    def acquire(self):
        pass

    def release(self):
        pass


class ThreadLimiter(object):
    """Limit threads for a block of code amongst multiple thread environment.

    TODO: update me
    """

    PREFIX = "KlioSemaphore"

    def __init__(self, max_thread_count=None):
        self.logger = logging.getLogger("klio.concurrency")

        if max_thread_count is None:
            max_thread_count = multiprocessing.cpu_count()
            self.logger.debug(
                f"{self} Defaulting to CPU count: {max_thread_count}"
            )

        if max_thread_count is False:
            self._dummy = True
            self._semaphore = _DummySemaphore()
            self.logger.debug(f"{self} Using dummy semaphore")
        else:
            self._dummy = False
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
        self._log("Blocked – waiting for semaphore")
        self._semaphore.acquire()

    def release(self):
        self._semaphore.release()
        self._log("Released semaphore")

    def __repr__(self):
        return f"{self.PREFIX}(id={id(self)})"

    def __enter__(self):
        self.acquire()
        return self._semaphore

    def __exit__(self, type, value, traceback):
        self.release()

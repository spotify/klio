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

import functools
import inspect

import memory_profiler as mp

from klio_exec.commands.utils import wrappers


class KMemoryLineProfiler(wrappers.KLineProfilerMixin, mp.LineProfiler):
    @classmethod
    def _wrap_per_element_func(cls, func, stream=None):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            prof = cls(backend="psutil")
            try:
                return prof(func)(*args, **kwargs)
            finally:
                mp.show_results(prof, stream=stream)

        return wrapper

    @classmethod
    def _wrap_per_element_gen(cls, func, stream=None):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            prof = cls(backend="psutil")
            try:
                yield from prof(func)(*args, **kwargs)
            finally:
                mp.show_results(prof, stream=stream)

        return wrapper

    @classmethod
    def wrap_per_element(cls, func, **kwargs):
        if inspect.isgeneratorfunction(func):
            return cls._wrap_per_element_gen(func, **kwargs)
        return cls._wrap_per_element_func(func, **kwargs)

    @staticmethod
    def _wrap_maximum_func(prof, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return prof(func)(*args, **kwargs)

        return wrapper

    @staticmethod
    def _wrap_maximum_gen(prof, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            yield from prof(func)(*args, **kwargs)

        return wrapper

    @classmethod
    def wrap_maximum(cls, prof, func, **_):
        if inspect.isgeneratorfunction(func):
            return cls._wrap_maximum_gen(prof, func)
        return cls._wrap_maximum_func(prof, func)

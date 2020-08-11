# Copyright 2020 Spotify AB

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

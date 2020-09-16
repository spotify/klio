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


def _get_transform_error_msg(txf=None, entity_id=None, err_msg=None):
    # This error message is printed instead of logged since user may not
    # run with logs turned on
    return (
        "WARN: Error caught while profiling {txf}.process for "
        "entity ID {entity_id}: {err_msg}".format(
            txf=txf, entity_id=entity_id, err_msg=err_msg
        )
    )


def _print_user_exceptions_generator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        transform_name = args[0].__class__.__name__
        entity_id = args[1]
        result = None
        try:
            result = yield from func(*args, **kwargs)

        except Exception as e:
            msg = _get_transform_error_msg(
                txf=transform_name, entity_id=entity_id, err_msg=e
            )
            print(msg)

        return result

    return wrapper


def _print_user_exceptions_func(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        transform_name = args[0].__class__.__name__
        entity_id = args[1]
        result = None
        try:
            result = func(*args, **kwargs)

        except Exception as e:
            msg = _get_transform_error_msg(
                txf=transform_name, entity_id=entity_id, err_msg=e
            )
            print(msg)

        return result

    return wrapper


def print_user_exceptions(transforms):
    # Don't crap out if the process method errors; just continue profiling
    for txf in transforms:
        process_method = getattr(txf, "process")
        if inspect.isgeneratorfunction(process_method):
            process_method = _print_user_exceptions_generator(process_method)
        else:
            process_method = _print_user_exceptions_func(process_method)
        setattr(txf, "process", process_method)
        yield txf


# adapted from line_profiler; memory_profiler doesn't handle generator
# functions for some reason.
class KLineProfilerMixin(object):
    """Mixin for CPU & Memory line profilers."""

    def __call__(self, func):
        # Overwrite to handle generators in the same fashion as funcs
        self.add_function(func)
        if inspect.isgeneratorfunction(func):
            return self.wrap_generator(func)
        return self.wrap_function(func)

    def wrap_function(self, func):
        """Wrap a function to profile it."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.enable_by_count()
            try:
                return func(*args, **kwargs)
            finally:
                self.disable_by_count()

        return wrapper

    def wrap_generator(self, func):
        """Wrap a generator to profile it."""

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.enable_by_count()
            try:
                yield from func(*args, **kwargs)
            finally:
                self.disable_by_count()

        return wrapper

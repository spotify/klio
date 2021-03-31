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

import collections
import contextlib
import functools
import inspect
import threading
import types

import apache_beam as beam
from apache_beam import pvalue

from klio import utils as kutils
from klio.message import serializer
from klio.transforms import _retry as kretry
from klio.transforms import _timeout as ktimeout
from klio.transforms import _utils as txf_utils
from klio.transforms import core


_ERROR_MSG_KMSG_FROM_BYTES = (
    "Dropping KlioMessage - exception occurred when serializing '{}' "
    "from bytes to a KlioMessage.\n"
    "Error: {}"
)
_ERROR_MSG_KMSG_TO_BYTES = (
    "Dropping KlioMessage - exception occurred when deserializing '{}' "
    "from a KlioMessage to bytes.\n"
    "Error: {}"
)
_CACHED_INSTANCES = collections.defaultdict(set)


# TODO: This may be nice to make generic and move into the public helpers
# module since users may want to use something like this.
def __get_or_create_instance(function):
    """Creates a context manager so we can avoid creating instances of
    the same object (in our case, KlioContext).

    This uses a lock so we can avoid threading issues in Dataflow.
    """
    lock = threading.Lock()

    @contextlib.contextmanager
    def func(*args, **kwargs):
        key = "KlioContext_" + str(id(function))
        try:
            instance = _CACHED_INSTANCES[key].pop()
        except KeyError:
            with lock:
                instance = function()
        try:
            yield instance
        finally:
            _CACHED_INSTANCES[key].add(instance)

    return func


@__get_or_create_instance
def _klio_context():
    return core.KlioContext()


def __is_method(obj):
    # we have to do this instead of inspect.ismethod since
    # it's not a "bounded" method, ie Foo.process (unbounded)
    # vs Foo().process (bounded)
    args = inspect.getfullargspec(obj).args
    if args:
        return "self" == args[0]
    return False


def __is_dofn_process_method(self, meth):
    if meth.__name__ == "process":
        if issubclass(self.__class__, beam.DoFn):
            return True
    return False


def __get_user_error_message(err, func_path, kmsg):
    tb, exc_info = "", True

    if hasattr(err, "_klio_traceback"):
        tb = "\n" + err._klio_traceback
        exc_info = False

    if isinstance(err, ktimeout.KlioTimeoutError):
        # no need to include traceback of a timeout error
        exc_info = False

    msg = (
        "Dropping KlioMessage - exception occurred when calling '%s' with "
        "'%s'.\nError: %s%s" % (func_path, kmsg, err, tb)
    )
    return msg, exc_info


def __get_thread_limiter(max_thread_count, thread_limiter, func_name=None):
    if max_thread_count is not None and thread_limiter is not None:
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "`max_thread_count` and `thread_limiter` are mutually exclusive "
            "arguments."
        )

    if thread_limiter is not None:
        if not isinstance(thread_limiter, kutils.ThreadLimiter):
            # raise a runtime error so it actually crashes klio/beam rather
            # than just continue processing elements
            raise RuntimeError(
                "'thread_limiter' must be an instance of `klio.utils."
                "ThreadLimiter`."
            )

    if max_thread_count is not None:
        is_int_enum = isinstance(max_thread_count, (int, kutils.ThreadLimit))
        is_func = callable(max_thread_count)
        if not any([is_int_enum, is_func]):
            # raise a runtime error so it actually crashes klio/beam rather
            # than just continue processing elements
            raise RuntimeError(
                "Invalid type for handle_klio's argument 'max_thread_count'. "
                "Expected an `int`, a callable returning an `int`, or "
                "`klio.utils.ThreadLimit`, got `%s`."
                % type(max_thread_count).__name__
            )

        if isinstance(max_thread_count, int) and max_thread_count <= 0:
            # raise a runtime error so it actually crashes klio/beam rather
            # than just continue processing elements
            raise RuntimeError(
                "'max_thread_count' must be greater than 0. Set "
                "'max_thread_count' to `None` or `klio.utils.ThreadLimiter."
                "NONE` to turn off thread limitations."
            )
    if max_thread_count is None and thread_limiter is None:
        max_thread_count = kutils.ThreadLimit.DEFAULT

    if thread_limiter is None:
        thread_limiter = kutils.ThreadLimiter(
            max_thread_count=max_thread_count, name=func_name
        )

    return thread_limiter


# A separate function from __serialize_klio_message_generator so we can
# specifically `yield from` it (and exhaust transforms that have multiple
# yields)
def __from_klio_message_generator(self, kmsg, payload, orig_item):
    try:
        yield serializer.from_klio_message(kmsg, payload)

    except Exception as err:
        self._klio.logger.error(
            _ERROR_MSG_KMSG_TO_BYTES.format(kmsg, err), exc_info=True
        )
        # Since the yielded value in the `try` clause may not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        # We won't try to serialize kmsg to bytes since something already
        # went wrong.
        yield pvalue.TaggedOutput("drop", orig_item)
        # explicitly return so that Beam doesn't call `next` and
        # executes the next `yield`
        return


# meant for DoFn.process generator methods; very similar to the
# '__serialize_klio_message' function, but needs to be its own function
# since it's a generator (can't mix the two - if a function has a `yield`
# statement, it automatically becomes a generator)
def __serialize_klio_message_generator(
    self, meth, incoming_item, *args, **kwargs
):
    try:
        kmsg = serializer.to_klio_message(
            incoming_item, self._klio.config, self._klio.logger
        )
    except Exception as err:
        self._klio.logger.error(
            _ERROR_MSG_KMSG_FROM_BYTES.format(incoming_item, err),
            exc_info=True,
        )
        # Since the yielded value in the `try` clause is not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        yield pvalue.TaggedOutput("drop", incoming_item)
        # explicitly return so that Beam doesn't call `next` and
        # executes the next `yield`
        return

    try:
        payload = meth(self, kmsg.data, *args, **kwargs)

    except Exception as err:
        func_path = self.__class__.__name__ + "." + meth.__name__
        log_msg, exc_info = __get_user_error_message(err, func_path, kmsg)
        self._klio.logger.error(log_msg, exc_info=exc_info)
        # Since the yielded value in the `try` clause is not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        # We won't try to serialize kmsg to bytes since something already
        # went wrong.
        yield pvalue.TaggedOutput("drop", incoming_item)
        # explicitly return so that Beam doesn't call `next` and
        # executes the next `yield`
        return

    else:
        if isinstance(payload, types.GeneratorType):
            try:
                for pl in payload:
                    yield from __from_klio_message_generator(
                        self, kmsg, pl, incoming_item
                    )
            # This exception block will the execute
            # if the pl item is an Exception
            except Exception as err:
                func_path = self.__class__.__name__ + "." + meth.__name__
                log_msg, exc_info = __get_user_error_message(
                    err, func_path, kmsg
                )
                self._klio.logger.error(log_msg, exc_info=exc_info)
                # This will catch an exception present in the generator
                # containing items yielded by a function/method
                # decorated by @handle_klio.
                # Following items in the generator will be ignored
                # since an exception has already been detected.
                # We won't try to serialize kmsg to bytes since
                # something already went wrong.
                yield pvalue.TaggedOutput("drop", incoming_item)
                # explicitly return so that Beam doesn't call `next` and
                # executes the next `yield`
                return
        else:
            yield from __from_klio_message_generator(
                self, kmsg, payload, incoming_item
            )


def __serialize_klio_message(ctx, func, incoming_item, *args, **kwargs):
    # manipulate `ctx` to handle both methods and functions depending on
    # what we're wrapping. Functions just have `ctx` object, but methods
    # have `self._klio` as its context, and we also need access to `self`
    # in order to call the method
    _self = ctx
    if not isinstance(ctx, core.KlioContext):
        ctx = _self._klio
    try:
        kmsg = serializer.to_klio_message(
            incoming_item, ctx.config, ctx.logger
        )
    except Exception as err:
        ctx.logger.error(
            _ERROR_MSG_KMSG_FROM_BYTES.format(incoming_item, err),
            exc_info=True,
        )
        # Since the returned value in the `try` clause is not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        return pvalue.TaggedOutput("drop", incoming_item)

    try:
        ret = func(_self, kmsg.data, *args, **kwargs)
        if isinstance(ret, types.GeneratorType):
            raise TypeError(
                "can't pickle generator object: '{}'".format(func.__name__)
            )
    except TypeError:
        # If we get here, we threw a type error because we found a generator
        # and those can't be pickled. But there's no need to do any special
        # error handling - this will contain enough info for the user so
        # we just re-raise
        raise

    except Exception as err:
        log_msg, exc_info = __get_user_error_message(err, func.__name__, kmsg)
        ctx.logger.error(log_msg, exc_info=exc_info)
        # Since the returned value in the `try` clause is not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        # We won't try to serialize kmsg to bytes since something already
        # went wrong.
        return pvalue.TaggedOutput("drop", incoming_item)

    try:
        return serializer.from_klio_message(kmsg, ret)

    except Exception as err:
        ctx.logger.error(
            _ERROR_MSG_KMSG_TO_BYTES.format(kmsg, err), exc_info=True
        )
        # Since the returned value in the `try` clause is not tagged, that
        # one will be used by default by whatever executed this function,
        # and anything that has a tagged output value (like this dropped one)
        # will just be ignored, which is fine for dropped values.
        # But if the caller function wanted to, they could access this via
        # pcoll.drop.
        # We won't try to serialize kmsg to bytes since something already
        # went wrong.
        return pvalue.TaggedOutput("drop", incoming_item)


def _serialize_klio_message(func_or_meth):
    @functools.wraps(func_or_meth)
    def method_wrapper(self, incoming_item, *args, **kwargs):
        wrapper = __serialize_klio_message
        wrapper_kwargs = {
            "ctx": self,
            "func": func_or_meth,
            "incoming_item": incoming_item,
        }
        if __is_dofn_process_method(self, func_wrapper):
            wrapper = __serialize_klio_message_generator
            wrapper_kwargs = {
                "self": self,
                "meth": func_or_meth,
                "incoming_item": incoming_item,
            }
        return wrapper(*args, **wrapper_kwargs, **kwargs)

    @functools.wraps(func_or_meth)
    def func_wrapper(klio_ns, incoming_item, *args, **kwargs):
        wrapper_kwargs = {
            "ctx": klio_ns,
            "func": func_or_meth,
            "incoming_item": incoming_item,
        }
        return __serialize_klio_message(*args, **wrapper_kwargs, **kwargs)

    if __is_method(func_or_meth):
        return method_wrapper
    return func_wrapper


def _set_klio_context(method):
    @functools.wraps(method)
    def method_wrapper(self, *args, **kwargs):
        if not getattr(self, "_klio", None):
            with _klio_context() as ctx:
                setattr(self, "_klio", ctx)
        return method(self, *args, **kwargs)

    return method_wrapper


def _inject_klio_context(func_or_meth):
    @functools.wraps(func_or_meth)
    def method_wrapper(self, *args, **kwargs):
        with _klio_context() as ctx:
            return func_or_meth(self, ctx, *args, **kwargs)

    @functools.wraps(func_or_meth)
    def func_wrapper(*args, **kwargs):
        with _klio_context() as ctx:
            return func_or_meth(ctx, *args, **kwargs)

    if __is_method(func_or_meth):
        return method_wrapper
    return func_wrapper


def _handle_klio(*args, max_thread_count=None, thread_limiter=None, **kwargs):
    def inner(func_or_meth):
        func_name = getattr(
            func_or_meth, "__qualname__", func_or_meth.__name__
        )
        thd_limiter = __get_thread_limiter(
            max_thread_count, thread_limiter, func_name
        )
        # grab klio context outside of the method/func wrappers so the
        # context manager isn't called for every time an item is processed
        with _klio_context() as ctx:
            kctx = ctx

        @functools.wraps(func_or_meth)
        def method_wrapper(self, *args, **kwargs):
            with thd_limiter:
                setattr(self, "_klio", kctx)

                # SO. HACKY. We check to see if this method is named "expand"
                # to designate  if the class is a Composite-type transform
                # (rather than a DoFn with a "process" method).
                # A Composite transform handles a pcoll / pipeline,
                # not the individual elements, and therefore doesn't need
                # to be given a KlioMessage. It should only need the KlioContext
                # attached.
                if func_or_meth.__name__ == "expand":
                    return func_or_meth(self, *args, **kwargs)

                incoming_item = args[0]
                args = args[1:]

                wrapper = __serialize_klio_message
                wrapper_kwargs = {
                    "ctx": self,
                    "func": func_or_meth,
                    "incoming_item": incoming_item,
                }
                # Only the process method of a DoFn is a generator - otherwise
                # beam can't pickle a generator
                if __is_dofn_process_method(self, func_or_meth):
                    wrapper = __serialize_klio_message_generator
                    wrapper_kwargs = {
                        "self": self,
                        "meth": func_or_meth,
                        "incoming_item": incoming_item,
                    }
                return wrapper(*args, **wrapper_kwargs, **kwargs)

        @functools.wraps(func_or_meth)
        def func_wrapper(incoming_item, *args, **kwargs):
            with thd_limiter:
                wrapper_kwargs = {
                    "ctx": kctx,
                    "func": func_or_meth,
                    "incoming_item": incoming_item,
                }
                return __serialize_klio_message(
                    *args, **wrapper_kwargs, **kwargs
                )

        if __is_method(func_or_meth):
            return method_wrapper
        return func_wrapper

    # allows @handle_klio to be used without parens (i.e. no need to do
    # `@handle_klio()`) when there are no args/kwargs provided
    if args and callable(args[0]):
        return inner(args[0])
    return inner


def _timeout(seconds=None, exception=None, exception_message=None):
    try:
        seconds = float(seconds)
    except ValueError:
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "Invalid type for timeout 'seconds'. Expected a `float` or an "
            "`int`, got `%s`." % type(seconds).__name__
        )

    if not seconds > 0:
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "Invalid value '%d' for timeout. Must be a positive number."
            % seconds
        )

    def inner(func_or_meth):
        with _klio_context() as kctx:
            timeout_wrapper = ktimeout.KlioTimeoutWrapper(
                function=func_or_meth,
                seconds=seconds,
                timeout_exception=exception,
                exception_message=exception_message,
                klio_context=kctx,
            )

        # Unfortunately these two wrappers can't be abstracted into
        # one wrapper - the `self` arg apparently can not be abstracted
        @functools.wraps(func_or_meth)
        def method_wrapper(self, kmsg, *args, **kwargs):
            args = (self, kmsg) + args
            return timeout_wrapper(*args, **kwargs)

        @functools.wraps(func_or_meth)
        def func_wrapper(ctx, kmsg, *args, **kwargs):
            args = (ctx, kmsg) + args
            return timeout_wrapper(*args, **kwargs)

        if __is_method(func_or_meth):
            return method_wrapper
        return func_wrapper

    return inner


def _retry(
    *args,
    tries=None,
    delay=None,
    exception=None,
    raise_exception=None,
    exception_message=None,
    **kwargs
):
    if tries is None:
        tries = -1  # infinite

    if not isinstance(tries, int):
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "Invalid type for retry 'tries'. Expected an int, got "
            "`{}`.".format(type(tries).__name__)
        )

    if tries < -1:
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "Invalid value '%d' for retry 'tries'. Must be a positive integer "
            "or '-1' for infinite tries." % tries
        )

    if delay is None:
        delay = 0

    if not isinstance(delay, (int, float)):
        raise RuntimeError(
            "Invalid type for retry 'delay'. Expected a `float` or an "
            "`int`, got `%s`." % type(delay).__name__
        )

    if delay < 0:
        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        raise RuntimeError(
            "Invalid value '%d' for retry 'delay'. Must be a positive number. "
            % delay
        )

    def inner(func_or_meth):
        with _klio_context() as kctx:
            retry_wrapper = kretry.KlioRetryWrapper(
                function=func_or_meth,
                tries=tries,
                delay=delay,
                exception=exception,
                raise_exception=raise_exception,
                exception_message=exception_message,
                klio_context=kctx,
            )

        # Unfortunately these two wrappers can't be abstracted into
        # one wrapper - the `self` arg apparently can not be abstracted
        @functools.wraps(func_or_meth)
        def method_wrapper(self, kmsg, *args, **kwargs):
            args = (self, kmsg) + args
            return retry_wrapper(*args, **kwargs)

        @functools.wraps(func_or_meth)
        def func_wrapper(ctx, kmsg, *args, **kwargs):
            args = (ctx, kmsg) + args
            return retry_wrapper(*args, **kwargs)

        if __is_method(func_or_meth):
            return method_wrapper
        return func_wrapper

    return inner


# Allow internals to call semiprivate funcs without triggering
# user-facing warnings
@txf_utils.experimental()
def serialize_klio_message(*args, **kwargs):
    """Serialize/deserialize incoming PCollections as a KlioMessage.

    This decorator needs access to a :class:`KlioContext
    <klio.transforms.core.KlioContext>` object via ``@inject_klio_context``
    or ``@set_klio_context`` if not available on the object (i.e. ``self`` of
    a DoFn instance), or use ``@handle_klio`` which will handle ``KlioContext``
    and KlioMessage de/serialization.
    """
    return _serialize_klio_message(*args, **kwargs)


@txf_utils.experimental()
def set_klio_context(*args, **kwargs):
    """Set :class:`KlioContext <klio.transforms.core.KlioContext>` to the
    class instance.

    Use ``@handle_klio`` instead if KlioMessage de/serialization is
    also needed.

    Use ``@inject_klio_context`` if using on a function rather than a
    class method.

    .. code-block:: python

        class MyComposite(beam.PTransform):
            @set_klio_context
            def expand(self, element):
                self._klio.logger.info(f"Received {element}")
    """
    return _set_klio_context(*args, **kwargs)


@txf_utils.experimental()
def inject_klio_context(*args, **kwargs):
    """Provide :class:`KlioContext <klio.transforms.core.KlioContext>` as the
    first argument to a decorated method/func.

    Use ``@handle_klio`` instead if KlioMessage de/serialization is
    also needed.

    If not needing KlioMessage de/serialization, consider
    ``@set_klio_context`` if using on a class method (rather than a
    function) to set the ``_klio`` attribute on ``self``.

    .. code-block:: python

        @inject_klio_context
        def my_map_func(ctx, element):
            ctx.logger.info(f"Received {element}")

        class MyDoFn(beam.DoFn):
            @inject_klio_context
            def process(self, ctx, element):
                ctx.logger.info(f"Received {element}")
    """
    return _inject_klio_context(*args, **kwargs)


# TODO: Update docstrings w/ new kwargs & examples
@txf_utils.experimental()
def handle_klio(*args, max_thread_count=None, thread_limiter=None, **kwargs):
    """Serialize & deserialize incoming PCollections as a KlioMessage.

    Behind the scenes, this generates :class:`KlioContext
    <klio.transforms.core.KlioContext>`, handles de/serialize the
    incoming PCollection as a Klio Message, as well as manage thread
    concurrency.

    .. admonition:: Default Thread Concurrency Management
        :class: caution

        The ``@handle_klio`` decorator will default to limiting the amount
        of active threads a decorated transform can use. The default
        maximum number of active threads is the number of CPUs on the
        worker machine.

        See examples below on how to adjust this behavior.

        Learn more about Klio's thread concurrency management
        :ref:`in the User Guide <klio-concurrency-mgmt>`.

    If decorating a class method, the ``KlioContext`` will be attached
    to the ``self`` argument of the class instance.

    If decorating a function, ``KlioContext`` will be provided as the first
    argument.

    .. code-block:: python

        @handle_klio
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")

        class MyDoFn(beam.DoFn):
            @handle_klio
            def process(self, item):
                self._klio.logger.info(
                    f"Received {item.element} with {item.payload}"
                )

        class MyComposite(beam.PTransform):
            @handle_klio
            def expand(self, pcoll):
                kms_config = self._klio.config.job_config.kms_config
                return pcoll | MyKMSTransform(**kms_config)

    To adjust the maximum threads a decorated transform uses:

    .. code-block:: python

        from klio import utils as klio_utils

        # Set the limit to 4 threads
        @handle_klio(max_thread_count=4):
        def my_map_func(ctx, item):
            ...

        # Set the limit to 2x CPU count
        import multiprocessing
        @handle_klio(max_thread_count=lambda: 2 * multiprocessing.cpu_count()):
        def my_map_func(ctx, item):
            ...

        # Turn off any thread limits
        @handle_klio(max_thread_count=klio_utils.ThreadLimit.NONE):
        def my_map_func(ctx, item):
            ...

        # Explicitly set the limit to Klio's default
        @handle_klio(max_thread_count=klio_utils.ThreadLimit.DEFAULT):
        def my_map_func(ctx, item):
            ...

        # Share thread limits between multiple transforms
        global_thread_limiter = klio_utils.ThreadLimiter(max_thread_count=4)

        @handle_klio(thread_limiter=global_thread_limiter)
        def first_map_func(ctx, item):
            ...

        @handle_klio(thread_limiter=global_thread_limiter)
        def second_map_func(ctx, item):
            ...

    Args:
        max_thread_count (int, callable, klio.utils.ThreadLimit): number of
            threads to make available to the decorated function, or a
            :func:`callable` that returns an ``int``. Set to
            :attr:`klio.utils.ThreadLimit.NONE` for no thread limits.
            Defaults to :attr:`klio.utils.ThreadLimit.DEFAULT` (worker CPU
            count) if ``thread_limiter`` is not provided.
            **Mutually exclusive** with ``thread_limiter`` argument.

        thread_limiter (klio.utils.ThreadLimiter): the ``ThreadLimiter``
            instance that the decorator should use instead of creating its own.
            Defaults to ``None``. **Mutually exclusive** with
            ``max_thread_count``.
    """
    return _handle_klio(
        *args,
        max_thread_count=max_thread_count,
        thread_limiter=thread_limiter,
        **kwargs,
    )


@txf_utils.experimental()
def timeout(seconds, *args, exception=None, exception_message=None, **kwargs):
    """Run the decorated method/function with a timeout in a separate process.

    If being used with :func:`@retry <retry>`, order is important depending
    on the desired effect. If ``@timeout`` is applied to a function before
    ``@retry``, then retries will apply first, meaning the configured timeout
    will cancel the function even if the retries have not yet been exhausted.
    In this case, be careful with the ``delay`` argument for the ``@retry``
    decorator: the set timeout is inclusive of a retry's delay. Conversely,
    if ``@retry`` is applied to a function before ``@timeout``, retries will
    continue until exhausted even if a function has timed out.

    If being used with another Klio decorator like :func:`@handle_klio
    <handle_klio>`, then the ``@timeout`` decorator should be applied to a
    method/function **after** another Klio decorator.

    .. code-block:: python

        @handle_klio
        @timeout(seconds=5)
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")

        class MyDoFn(beam.DoFn):
            @handle_klio
            @timeout(seconds=5, exception=MyTimeoutException)
            def process(self, item):
                self._klio.logger.info(
                    f"Received {item.element} with {item.payload}"
                )

        @timeout(
            seconds=5,
            exception=MyTimeoutException,
            exception_message="I got a timeout!"
        )
        def my_nonklio_map_func(item):
            print(f"Received {item}!")

    Args:
        seconds (float): The timeout period in seconds. Must be greater than 0.
        exception (Exception): The Exception that will be raised if a
            timeout occurs. Default: ``KlioTimeoutError``.
        exception_message (str): Custom exception message. Default:
            ``Function '{function}' timed out after {seconds} seconds.``
    """
    return _timeout(
        *args,
        seconds=seconds,
        exception=exception,
        exception_message=exception_message,
        **kwargs,
    )


@txf_utils.experimental()
def retry(
    *args,
    tries=None,
    delay=None,
    exception=None,
    raise_exception=None,
    exception_message=None,
    **kwargs
):
    """Retry a decorated method/function on failure.

    If being used with :func:`@timeout <timeout>`, order is important
    depending on the desired effect. If ``@timeout`` is applied to a function
    before ``@retry``, then retries will apply first, meaning the configured
    timeout will cancel the function even if the retries have not yet been
    exhausted. In this case, be careful with the ``delay`` argument for the
    ``@retry`` decorator: the set timeout is inclusive of a retry's delay.
    Conversely, if ``@retry`` is applied to a function before ``@timeout``,
    retries will continue until exhausted even if a function has timed out.

    If being used with a Klio decorator like :func:`@handle_klio
    <handle_klio>`, then the ``@retry`` decorator should be applied to a
    method/function **after** another Klio decorator.

    .. code-block:: python

        @handle_klio
        @retry()
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")

        class MyDoFn(beam.DoFn):
            @handle_klio
            @retry(tries=3, exception=MyExceptionToCatch)
            def process(self, item):
                self._klio.logger.info(
                    f"Received {item.element} with {item.payload}"
                )

        @retry(
            tries=3,
            exception=MyExceptionToCatch,
            raise_exception=MyExceptionToRaise,
            exception_message="All retries have been exhausted!"
        )
        def my_nonklio_map_func(item):
            print(f"Received {item}!")

    Args:
        tries (int): Maximum number of attempts. Default: -1 (infinite)
        delay (int or float): Delay between attempts in seconds. Default: 0
        exception (Exception or tuple(Exception)): An Exception or tuple of
            exceptions to catch and retry on. Defaults to ``Exception``.
        raise_exception (Exception): The ``Exception`` to raise once configured
            retries are exhausted. Defaults to ``KlioRetriesExhausted``.
        exception_message (str): Custom message for `raise_exception`.
            Default: ``Function '{}' has reached the maximum {} retries. Last
            exception: {}``

    """
    # There isn't a way to nicely support `@retry` because it might be used
    # with other decorators which adds a lot of complexity, so it must be
    # `@retry()`. But we can detect it and give a more friendly error.
    if len(args) == 1 and callable(args[0]):
        func_name = getattr(args[0], "__qualname__", args[0].__name__)

        raise RuntimeError(
            "The `retry` decorator needs to be called with parens on '{}', "
            "e.g. `@retry()`.".format(func_name)
        )

    return _retry(
        *args,
        tries=tries,
        delay=delay,
        exception=exception,
        raise_exception=raise_exception,
        exception_message=exception_message,
        **kwargs,
    )


# this is set by the exec profile command when it's time to profile stuff.
# When set, this should be a 1-arg function that takes another function as
# input and returns a wrapped version of it with profiling
ACTIVE_PROFILER = None


@txf_utils.experimental()
def profile(func_or_meth):
    """Decorator to mark a function/method for profiling.  This is used in
    conjunction with the ``klio job profile`` commands to selectively profile
    parts of your pipeline.  This decorator can be added to any function or
    method, but when using with other Klio decorators such as ``@handle_klio``
    it **must** be the last decorator applied.

    When running/testing a job normally and not profiling, this decorator has
    no effect.

    .. code-block:: python

        @handle_klio
        @profile
        def my_map_func(ctx, item):
            ctx.logger.info(f"Received {item.element} with {item.payload}")

        class MyDoFn(beam.DoFn):
            @handle_klio
            @profile
            def process(self, item):
                self._klio.logger.info(
                    f"Received {item.element} with {item.payload}"
                )

        @profile
        def my_nonklio_map_func(item):
            print(f"Received {item}!")
    """

    if ACTIVE_PROFILER is not None:
        decorated = ACTIVE_PROFILER(func_or_meth)

        @functools.wraps(func_or_meth)
        def inner_method(self, *args, **kwargs):
            return decorated(self, *args, **kwargs)

        @functools.wraps(func_or_meth)
        def inner_func(*args, **kwargs):
            return decorated(*args, **kwargs)

        # This is needed even though we treat them the same, other decorators
        # need to do the same check and won't work if we don't propagate the
        # `self` arg (which is not done by functools.wraps)
        if __is_method(func_or_meth):
            return inner_method
        else:
            return inner_func

    return func_or_meth

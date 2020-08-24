# Copyright 2020 Spotify AB

import collections
import contextlib
import functools
import inspect
import threading
import types

import apache_beam as beam
from apache_beam import pvalue

from klio.message import serializer
from klio.transforms import _timeout as ktimeout
from klio.transforms import _utils
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
    args = inspect.getargspec(obj).args
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
            for pl in payload:
                yield from __from_klio_message_generator(
                    self, kmsg, pl, incoming_item
                )
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
        if __is_dofn_process_method(self, func_wrapper):
            wrapper = __serialize_klio_message_generator

        return wrapper(self, func_or_meth, incoming_item, *args, **kwargs)

    @functools.wraps(func_or_meth)
    def func_wrapper(klio_ns, incoming_item, *args, **kwargs):
        return __serialize_klio_message(
            func_or_meth, klio_ns, incoming_item, *args, **kwargs
        )

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


def _handle_klio(func_or_meth):
    @functools.wraps(func_or_meth)
    def method_wrapper(self, *args, **kwargs):
        with _klio_context() as ctx:
            setattr(self, "_klio", ctx)

        # SO. HACKY. We check to see if this method is named "expand"
        # to designate  if the class is a Composite-type transform
        # (rather than a DoFn with a "process" method).
        # A Composite transform handles a pcoll / pipeline,
        # not the individual elements, and therefore doesn't need
        # to be given a KlioMessage. It should only need the KlioContext
        # attached.
        if func_or_meth.__name__ == "expand":
            return func_or_meth(self, *args, **kwargs)

        wrapper = __serialize_klio_message
        # Only the process method of a DoFn is a generator - otherwise
        # beam can't pickle a generator
        if __is_dofn_process_method(self, func_or_meth):
            wrapper = __serialize_klio_message_generator

        incoming_item = args[0]
        args = args[1:]
        return wrapper(self, func_or_meth, incoming_item, *args, **kwargs)

    @functools.wraps(func_or_meth)
    def func_wrapper(incoming_item, *args, **kwargs):
        with _klio_context() as ctx:
            return __serialize_klio_message(
                ctx, func_or_meth, incoming_item, *args, **kwargs
            )

    if __is_method(func_or_meth):
        return method_wrapper
    return func_wrapper


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
        timeout_wrapper = ktimeout.KlioTimeoutWrapper(
            function=func_or_meth,
            seconds=seconds,
            timeout_exception=exception,
            exception_message=exception_message,
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


# Allow internals to call semiprivate funcs without triggering
# user-facing warnings
@_utils.experimental()
def serialize_klio_message(*args, **kwargs):
    """Serialize/deserialize incoming pcollections as a KlioMessage.

    This decorator needs access to a KlioContext object via
    `@inject_klio_context` or `@set_klio_context` if not available on
    the object (i.e. `self` of a DoFn instance), or use `@handle_klio`
    which will handle KlioContext and KlioMessage de/serialization.
    """
    return _serialize_klio_message(*args, **kwargs)


@_utils.experimental()
def set_klio_context(*args, **kwargs):
    """Set Klio context to the class instance.

    Use `@handle_klio` instead if KlioMessage de/serialization is
    also needed.

    Use `@inject_klio_context` if using on a function rather than a
    class method.

    class MyComposite(beam.PTransform):
        @set_klio_context
        def expand(self, element):
            self._klio.logger.info(f"Received {element}")
    """
    return _set_klio_context(*args, **kwargs)


@_utils.experimental()
def inject_klio_context(*args, **kwargs):
    """Provide Klio context as the first argument to a decorated method/func.

    Use `@handle_klio` instead if KlioMessage de/serialization is
    also needed.

    If not needing KlioMessage de/serialization, consider
    `@set_klio_context` if using on a class method (rather than a
    function) to set the `_klio` attribute on `self`.

    @inject_klio_context
    def my_map_func(ctx, element):
        ctx.logger.info(f"Received {element}")

    class MyDoFn(beam.DoFn):
        @inject_klio_context
        def process(self, ctx, element):
            ctx.logger.info(f"Received {element}")
    """
    return _inject_klio_context(*args, **kwargs)


@_utils.experimental()
def handle_klio(*args, **kwargs):
    """Serialize & deserialize incoming pcollections as a KlioMessage.

    Behind the scenes, this generates Klio context as well as handles
    de/serialize the incoming pcollection as a Klio Message.

    If decorating a class method, the Klio context will be attached
    to the `self` argument of the class instance.

    If decorating a function, Klio context will be provided as the first
    argument.

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
    """
    return _handle_klio(*args, **kwargs)


@_utils.experimental()
def timeout(seconds, *args, exception=None, exception_message=None, **kwargs):
    """Run the decorated method/function with a timeout in a separate process.

    If being used with another Klio decorator like `@handle_klio`, then
    the `@timeout` decorator should be applied to a method/function
    **after** another Klio decorator.

    @handle_klio
    @timeout(seconds=5)
    def my_map_func(ctx, item):
        ctx.logger.info(f"Received {item.element} with {item.payload}")

    class MyDoFn(beam.DoFn):
        @handle_klio
        @timeou(seconds=5, exception=MyTimeoutException)
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
            timeout occurs. Default: `KlioTimeoutError`.
        exception_message (str): Custom exception message. Default:
            `Function '{function}' timed out after {seconds} seconds.`

    """
    return _timeout(
        *args,
        seconds=seconds,
        exception=exception,
        exception_message=exception_message,
        **kwargs,
    )

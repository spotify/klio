# Copyright 2020 Spotify AB

import collections
import functools
import inspect
import io
import pickle
import types

import apache_beam as beam
import numpy as np

from apache_beam import pvalue

from klio.transforms import core

# Actual KlioMessage will not take payload that's not bytes (i.e. numpy
# arrays), so we just re-wrap it into a faked klio-like message (just the
# data part) before handing it off to the user
FakeKlioMsg = collections.namedtuple("FakeKlioMsg", ["element", "payload"])


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


class _BinarySerializer(object):
    def __init__(self, skip_load, skip_dump, load_with_numpy, save_with_numpy):
        self.skip_load = skip_load
        self.skip_dump = skip_dump
        self.load_with_numpy = load_with_numpy
        self.save_with_numpy = save_with_numpy

    @staticmethod
    def _pickle_load(ctx, kmsg):
        try:
            payload = pickle.loads(kmsg.payload)
            return FakeKlioMsg(element=kmsg.element, payload=payload)

        except Exception as err:
            ctx.logger.error(
                "Exception occurred when unpickling payload for '%s'.\n"
                "Error: %s" % (kmsg.element, err)
            )
            raise err

    @staticmethod
    def _numpy_load(ctx, kmsg):
        try:
            in_ = io.BytesIO(kmsg.payload)
            in_.seek(0)  # push the read pointer to the beginning
            payload = np.load(in_, allow_pickle=True)
            return FakeKlioMsg(element=kmsg.element, payload=payload)

        except Exception as err:
            ctx.logger.error(
                "Exception occurred when loading numpy payload for '%s'.\n"
                "Error: %s" % (kmsg.element, err)
            )
            raise err

    @staticmethod
    def _pickle_dump(ctx, kmsg, ret):
        tagged, tag = False, None
        if isinstance(ret, pvalue.TaggedOutput):
            tagged = True
            tag = ret.tag
            ret = ret.value
        try:
            dumped = pickle.dumps(ret)
            if tagged:
                return pvalue.TaggedOutput(tag, dumped)
            return dumped
        except Exception as err:
            ctx.logger.error(
                "Exception occurred when pickling payload for '%s'.\nError: %s"
                % (kmsg.element, err)
            )
            raise err

    @staticmethod
    def _numpy_dump(ctx, kmsg, ret):
        tagged, tag = False, None
        if isinstance(ret, pvalue.TaggedOutput):
            tagged = True
            tag = ret.tag
            ret = ret.value
        try:
            out = io.BytesIO()
            np.save(out, ret)
            dumped = out.getvalue()  # returns the data in `out` in bytes
            if tagged:
                return pvalue.TaggedOutput(tag, dumped)
            return dumped
        except Exception as err:
            ctx.logger.error(
                "Exception occurred when dumping numpy payload for '%s'.\n"
                "Error: %s" % (kmsg.element, err)
            )
            raise err

    def load(self, ctx, kmsg):
        if self.skip_load:
            return kmsg

        if self.load_with_numpy:
            return self._numpy_load(ctx, kmsg)
        return self._pickle_load(ctx, kmsg)

    def dump(self, ctx, kmsg, ret_val):
        if self.skip_dump:
            return ret_val

        if self.save_with_numpy:
            return self._numpy_dump(ctx, kmsg, ret_val)
        return self._pickle_dump(ctx, kmsg, ret_val)


# A separate function from __handle_binary_generator so we can specifically
# `yield from` it (and exhaust transforms that have multiple yields)
def __yield_from_handle_binary(serializer, self, new_msg, ret_generator):
    for ret in ret_generator:
        ret = serializer.dump(self._klio, new_msg, ret)
        yield ret


def __handle_binary_generator(self, meth, kmsg, serializer, *args, **kwargs):
    new_msg = serializer.load(self._klio, kmsg)

    # any error should be caught further up (i.e. by `@handle_klio`)
    ret = meth(self, new_msg, *args, **kwargs)
    if not isinstance(ret, types.GeneratorType):
        ret = serializer.dump(self._klio, new_msg, ret)
        yield ret

    else:
        yield from __yield_from_handle_binary(serializer, self, new_msg, ret)


def __handle_binary(ctx, func, kmsg, serializer, *args, **kwargs):
    _self = ctx
    if not isinstance(ctx, core.KlioContext):
        ctx = _self._klio

    new_msg = serializer.load(ctx, kmsg)

    # any error should be caught further up (i.e. by `@handle_klio`)
    ret = func(ctx, new_msg, *args, **kwargs)

    ret = serializer.dump(ctx, new_msg, ret)
    return ret


def handle_binary(*decorator_args, **decorator_kwargs):
    """Decorator to handle the required loading/unloading of binary data.

    .. caution::

        The ``@handle_binary`` decorator **must** be used in conjunction with
        the :func:`@handle_klio <klio.transforms.decorators.handle_klio>`
        decorator. As well, ``@handle_binary`` **must** also be applied
        **after** ``@handle_klio``.

    Example usage:

    .. code-block:: python

        class MelSpectrogram(beam.DoFn):
            @handle_klio
            @handle_binary
            def process(self, item):
                self._klio.logger.info(
                    f"Generating specgram for {item.element}"
                )
                audio = item.payload
                yield librosa.feature.melspectrogram(y=audio, sr=22050)


        @handle_klio
        @handle_binary(skip_dump=True)
        def save_plt_as_png(ctx, item):
            fig = item.payload
            output = os.path.join(".", item.element.decode("utf-8") + ".png")
            plt.savefig(output, format="png", transparent=True, pad_inches=0)
            ctx.logger.info(f"Saved spectrogram: {output}")
            return output


        class DownloadAudio(beam.DoFn):
            def setup(self):
                self.client = SomeClient()

            @handle_klio
            @handle_binary(skip_load=True, save_with_numpy=True)
            def process(self, item):
                self._klio.logger.info(f"Downloading {item.element}")
                filename = item.payload.decode("utf-8")
                location = self._klio.config.job_config.data.inputs[0].location
                source_path = os.path.join(location, filename)
                with self.client.open(source_path, "rb") as source:
                    out = io.BytesIO(source.read())
                self._klio.logger.info(f"Downloaded {item.element} to memory")
                yield out

    Args:
        skip_load (bool): Skip loading the ``KlioMessage`` payload via pickle.
            Set this to ``True`` if the incoming ``KlioMessage`` payload is not
            binary data, or otherwise has not been pickled to bytes.
            Default: ``False``
        skip_dump (bool): Skip dumping the ``KlioMessage`` payload via pickle.
            Set this to ``True`` if the outgoing ``KlioMessage`` payload is not
            binary data, or otherwise should not be pickled to bytes.
            Default: ``False``
        load_with_numpy (bool): Use :func:`numpy.load` instead of
            :func:`pickle.loads` to load arrays or pickled numpy objects. This
            is less performant than standard pickling, but uses less memory.
            Default: ``False``.
        save_with_numpy (bool): Use :func:`numpy.save` instead of
            :func:`pickle.dumps` to save arrays as ``bytes``. This is less
            performant than standard pickling, but uses less memory.
            Default: ``False``
    """
    skip_load = decorator_kwargs.pop("skip_load", False)
    skip_dump = decorator_kwargs.pop("skip_dump", False)
    load_with_numpy = decorator_kwargs.pop("load_with_numpy", False)
    save_with_numpy = decorator_kwargs.pop("save_with_numpy", False)

    serializer = _BinarySerializer(
        skip_load, skip_dump, load_with_numpy, save_with_numpy
    )

    def inner(func_or_meth):
        @functools.wraps(func_or_meth)
        def method_wrapper(self, kmsg, *args, **kwargs):
            wrapper = __handle_binary
            # Only the process method of a DoFn is a generator - otherwise
            # beam can't pickle a generator
            if __is_dofn_process_method(self, func_or_meth):
                wrapper = __handle_binary_generator

            return wrapper(
                self, func_or_meth, kmsg, serializer, *args, **kwargs
            )

        @functools.wraps(func_or_meth)
        def func_wrapper(ctx, kmsg, *args, **kwargs):
            return __handle_binary(
                ctx, func_or_meth, kmsg, serializer, *args, **kwargs
            )

        if __is_method(func_or_meth):
            return method_wrapper
        return func_wrapper

    # allows @handle_binary to be used without parens (i.e. no need to do
    # `@handle_binary()`) when there are no args/kwargs provided
    if decorator_args:
        return inner(decorator_args[0])
    return inner

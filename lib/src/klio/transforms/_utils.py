# Copyright 2020 Spotify AB

"""For internal use only; no backwards-compatibility guarantees."""

import enum
import functools
import warnings


class AnnotatedStates(enum.Enum):
    DEPRECATED = "deprecated"
    EXPERIMENTAL = "experimental"


# adapted from https://github.com/apache/beam/blob/9c3941fc/
#                      sdks/python/apache_beam/utils/annotations.py
class KlioDeprecationWarning(DeprecationWarning):
    """Klio-specific deprecation warnings."""


class KlioFutureWarning(FutureWarning):
    """Klio-specific deprecation warnings."""


# Don't ignore klio deprecation warnings! (future warnings ok)
warnings.simplefilter("once", KlioDeprecationWarning)


def has_abstract_methods_implemented(cls, name, bases):
    """Verify a given class has its abstract methods implemented."""
    for base in bases:
        abstract_methods = getattr(base, "_klio_abstract_methods", [])
        class_methods = getattr(cls, "_klio_all_methods", [])
        for method in abstract_methods:
            if method not in class_methods:
                err_str = (
                    "Error instantiating class '{0}'. Implementation of "
                    "abstract method '{1}' from base class '{2}' is "
                    "required.".format(name, method, base.__name__)
                )
                raise NotImplementedError(err_str)


def get_all_methods(clsdict):
    return [name for name, val in clsdict.items() if callable(val)]


def get_abstract_methods(clsdict):
    return [
        name
        for name, val in clsdict.items()
        if callable(val) and getattr(val, "__isabstractklio__", False)
    ]


def abstract(meth):
    """Set a method as abstract."""
    # differentiating from abc.abstractmethod which sets __isabstractmethod__
    meth.__isabstractklio__ = True
    return meth


def is_original_process_func(clsdict, bases, base_class=None):
    """Only wrap the original `process` function.

    Without these (minimal) checks, the `process` function would be
    wrapped at least twice (the original `process` function from the
    user's DoFn, and our wrapped/decorated one), essentially causing
    any call to `process` (and the decorator) to be called at least
    twice.

    Args:
        clsdict (dict): dictionary of items for the class being
            instantiated.
        bases (tuple(class)): base class(es) of the class being
            instantiated.
    Returns:
        (bool) whether or not to wrap the `process` method of the class
            being instantiated.
    """
    if "process" not in clsdict:
        return False
    # ignore classes that don't inherit from our base class
    base_cls_names = [b.__name__ for b in bases]
    if base_class and base_class not in base_cls_names:
        return False
    # if the value of clsdict["process"] is not a meth/func
    if not callable(clsdict["process"]):
        return False
    # if the value of clsdict["process"] is already "new_process"
    if getattr(clsdict["process"], "__name__") != "process":
        return False
    return True


# adapted from https://github.com/apache/beam/blob/9c3941fc/
#                      sdks/python/apache_beam/utils/annotations.py
def annotate(state, since=None, current=None, message=None):
    """Decorates an API with a `deprecated` or `experimental` annotation.

    When a user uses a objected decorated with this annotation, they
    will see a `KlioFutureWarning` or `KlioDeprecationWarning` during
    runtime.

    Args:
        state (AnnotatedStates): the kind of annotation (AnnotatedStates.
            DEPRECATED or AnnotatedStates.EXPERIMENTAL).
        since: the version that causes the annotation (used for
            AnnotatedStates.DEPRECATED when no `message` is given;
            ignored for AnnotatedStates.EXPERIMENTAL).
        current: the suggested replacement function.
        message: if the default message does not suffice, the message
            can be changed using this argument. Default message for

    Returns:
        The decorator for the API.
    """

    def wrapper(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            warning_type = KlioFutureWarning
            if state == AnnotatedStates.DEPRECATED:
                warning_type = KlioDeprecationWarning

            warn_message = message
            if message is None:
                addl_ctx = (
                    " and is subject to incompatible changes, or removal "
                    "in a future release of Klio."
                )
                if state == AnnotatedStates.DEPRECATED:
                    _since = " since {}".format(since) if since else ""
                    _current = (
                        ". Use {} instead".format(current) if current else ""
                    )
                    addl_ctx = "{}{}.".format(_since, _current)

                msg_kwargs = {
                    "obj": func.__name__,
                    "annotation": state.value,
                    "addl_ctx": addl_ctx,
                }
                warn_message = "'{obj}' is {annotation}{addl_ctx}".format(
                    **msg_kwargs
                )

            warnings.warn(warn_message, warning_type, stacklevel=2)
            return func(*args, **kwargs)

        return inner

    return wrapper


# partials for ease of use
deprecated = functools.partial(annotate, state=AnnotatedStates.DEPRECATED)
experimental = functools.partial(
    annotate, state=AnnotatedStates.EXPERIMENTAL, since=None
)

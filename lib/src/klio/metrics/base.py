# Copyright 2019-2020 Spotify AB
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
Base classes from which a metrics consumer (i.e. Stackdriver, ffwd, etc.)
will need to implement.

New consumers are required to implement the :class:`AbstractRelayClient`, and
three metrics objects based off of :class:`BaseMetric`: a counter, a gauge, and
a timer.
"""
import abc

import six


class _DummyAttribute(object):
    # for the ability to do `FOO_ATTR = abstract_attr()` as well as
    # decorate a property method
    pass


def abstract_attr(obj=None):
    """Set an attribute or a property as abstract.

    Supports class-level attributes as well as methods defined as a
    ``@property``.

    Usage:

    .. code-block:: python

        class Foo(object):
            my_foo_attribute = abstract_attr()

            @property
            @abstract_attr
            def my_foo_property(self):
                pass

    Args:
        obj (callable): Python object to "decorate", i.e. a class method. If
            none is provided, a dummy object is created in order to attach
            the ``__isabstractattr__`` attribute (similar to
            ``__isabstractmethod__`` from ``abc.abstractmethod``).

    Returns object with ``__isabstractattr__`` attribute set to ``True``.
    """
    if not obj:
        obj = _DummyAttribute()
    obj.__isabstractattr__ = True
    return obj


def _has_abstract_attributes_implemented(cls, name, bases):
    """Verify a given class has its abstract attributes implemented."""
    for base in bases:
        abstract_attrs = getattr(base, "_klio_metrics_abstract_attributes", [])
        class_attrs = getattr(cls, "_klio_metrics_all_attributes", [])
        for attr in abstract_attrs:
            if attr not in class_attrs:
                err_str = (
                    "Error instantiating class '{0}'. Implementation of "
                    "abstract attribute '{1}' from base class '{2}' is "
                    "required.".format(name, attr, base.__name__)
                )
                raise NotImplementedError(err_str)


def _get_all_attributes(clsdict):
    return [name for name, val in six.iteritems(clsdict) if not callable(val)]


def _get_abstract_attributes(clsdict):
    return [
        name
        for name, val in six.iteritems(clsdict)
        if not callable(val) and getattr(val, "__isabstractattr__", False)
    ]


class _ABCBaseMeta(abc.ABCMeta):
    """Enforce behavior upon implementations of ABC classes."""

    def __init__(cls, name, bases, clsdict):
        _has_abstract_attributes_implemented(cls, name, bases)

    def __new__(metaclass, name, bases, clsdict):
        clsdict[
            "_klio_metrics_abstract_attributes"
        ] = _get_abstract_attributes(clsdict)
        clsdict["_klio_metrics_all_attributes"] = _get_all_attributes(clsdict)
        cls = super(_ABCBaseMeta, metaclass).__new__(
            metaclass, name, bases, clsdict
        )
        return cls


class AbstractRelayClient(six.with_metaclass(_ABCBaseMeta)):
    """Abstract base class for all metric consumer relay clients.

    Each new consumer (i.e. Stackdriver, ffwd, logging-based metrics)
    will need to implement this relay class.

    Attributes:
        RELAY_CLIENT_NAME (str): must match the key in ``klio-job.yaml``
            under ``job_config.metrics``.

    """

    RELAY_CLIENT_NAME = abstract_attr()

    def __init__(self, klio_config):
        self.klio_config = klio_config

    @abc.abstractmethod
    def unmarshal(self, metric):
        """Returns a dictionary-representation of the ``metric`` object"""
        pass

    @abc.abstractmethod
    def emit(self, metric):
        """Emit the given metric object to the particular consumer.

        ``emit`` will be run in a threadpool separate from the transform,
        and any errors raised from the method will be logged then ignored.
        """
        pass

    @abc.abstractmethod
    def counter(self, name, value=0, transform=None, **kwargs):
        """Return a newly instantiated counter-type metric specific for
        the particular consumer.

        Callers to the ``counter`` method will store new counter objects
        returned in memory for simple caching.
        """
        pass

    @abc.abstractmethod
    def gauge(self, name, value=0, transform=None, **kwargs):
        """Return a newly instantiated gauge-type metric specific for
        the particular consumer.

        Callers to the ``gauge`` method will store new gauge objects
        returned in memory for simple caching.
        """
        pass

    @abc.abstractmethod
    def timer(self, name, transform=None, **kwargs):
        """Return a newly instantiated timer-type metric specific for
        the particular consumer.

        Callers to the ``timer`` method will store new timer objects
        returned in memory for simple caching.
        """
        pass


class BaseMetric(object):
    """Base class for all metric types.

    A consumer must implement a counter metric, a gauge metric, and a
    timer metric.
    """

    def __init__(self, name, value=0, transform=None, **kwargs):
        self.name = name
        self.value = value
        self.transform = transform

    def update(self, value):
        self.value = value

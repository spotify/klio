# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import functools

from google.api_core import exceptions as gapi_exceptions
from google.cloud import pubsub


def _name(name):
    return "klio_global_state_%s" % name


def set_global(name, value):
    globals()[_name(name)] = value


def get_global(name):
    return globals().get(_name(name), None)


def delete_global(name):
    if _name(name) in globals():
        del globals()[_name(name)]


def get_or_initialize_global(name, initializer):
    value = get_global(name)
    if value is not None:
        return value
    if callable(initializer):
        value = initializer()
    else:
        value = initializer
    set_global(name, value)
    return value


def _get_publisher(topic):
    publisher = pubsub.PublisherClient()
    try:
        publisher.create_topic(topic)
    except gapi_exceptions.AlreadyExists:
        pass

    except Exception:
        raise  # to be handled by caller

    return publisher


def get_publisher(topic):
    """Get a publisher client for a given topic.

    Will first check if there is an already initialized client in the
    global namespace. Otherwise, initialize one then set it in the
    global namespace to avoid redundant initialization.

    Args:
        topic (str): Pub/Sub topic for the client with which to be
            initialized.
    Returns:
        An instance of ``pubsub.PublisherClient``.
    """
    key = "publisher_{}".format(topic)
    initializer = functools.partial(_get_publisher, topic)
    return get_or_initialize_global(key, initializer)


# makeshift Enums for py2 support
# my_enum = enum("FOO", "BAR") -> 0=my_enum.FOO, 1=my_enum.BAR
def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type("Enum", (), enums)

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
"""Utility functions for use within the Klio ecosystem."""

import functools

from google.api_core import exceptions as gapi_exceptions
from google.cloud import pubsub


def _name(name):
    return "klio_global_state_%s" % name


def set_global(name, value):
    """Set a variable in the global namespace.

    Args:
        name (str): name of global variable.
        value (Any): value of global variable.
    """
    globals()[_name(name)] = value


def get_global(name):
    """Get a variable from the global namespace.

    Args:
        name (str): name of global variable.
    Returns:
        Any: value of global variable, or ``None`` if not set.
    """
    return globals().get(_name(name), None)


def delete_global(name):
    """Delete a variable from the global namespace.

    Args:
        name (str): name of global variable.
    """
    if _name(name) in globals():
        del globals()[_name(name)]


def get_or_initialize_global(name, initializer):
    """Get a global variable, initializing if does not exist.

    .. caution::

        Global variables may cause problems for jobs in Dataflow,
        particularly where jobs use more than one thread.

    Args:
        name (str): name of global variable.
        initializer (Any): initial value if ``name`` does not exist in the
            global namespace.

    Returns:
        Any: Value of the global variable.
    """
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
        google.cloud.pubsub_v1.publisher.client.Client: an initialized
        client for publishing to Google Pub/Sub.
    """
    key = "publisher_{}".format(topic)
    initializer = functools.partial(_get_publisher, topic)
    return get_or_initialize_global(key, initializer)


# makeshift Enums for py2 support
# my_enum = enum("FOO", "BAR") -> 0=my_enum.FOO, 1=my_enum.BAR
def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type("Enum", (), enums)

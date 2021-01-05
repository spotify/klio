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
import logging
import os
import warnings

import attr
import yaml

from google.api_core import exceptions as gapi_exceptions
from google.cloud import pubsub

from klio_core import config
from klio_core import options


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


#############################
# Config utils
#############################
def get_config_by_path(config_filepath, parse_yaml=True):
    """Read in the file given at specified config path

    Args:
        config_filepath (str): File path to klio config file
        parse_yaml (bool): Whether to parse the given file path
            as yaml
    Returns:
        python object of yaml config file or bytes read from file
    """
    try:
        with open(config_filepath) as f:
            if parse_yaml:
                return yaml.safe_load(f)
            else:
                return f.read()
    except IOError:
        logging.error("Could not read config file {0}".format(config_filepath))
        raise SystemExit(1)


def get_config_job_dir(job_dir, config_file):
    """Read in the file given at specified config path

    Args:
        job_dir (str): File directory path to klio config file
        config_file (str): File name of config file, if not provided
            ``klio-job.yaml`` will be used
    Returns:
        job_dir - Absolute path to config file directory
        config_file - Path to config file
    """
    if job_dir and config_file:
        config_file = os.path.join(job_dir, config_file)

    elif not job_dir:
        job_dir = os.getcwd()

    job_dir = os.path.abspath(job_dir)

    if not config_file:
        config_file = os.path.join(job_dir, "klio-job.yaml")

    return job_dir, config_file


@attr.attrs
class KlioConfigMeta(object):
    """
    Meta data about klio config
        job_dir: resolved directory of job
        config_path: resolved path to config file
        config_file: user-override of config file (may be None)
    """

    # resolved directory of job
    job_dir = attr.attrib()

    # resolved path to config file
    config_path = attr.attrib()

    # user-override of config file (may be None)
    config_file = attr.attrib()


def with_klio_config(func):
    """Decorator for commands to automatically handle a number of options
    regarding config, and provide a properly constructed KlioConfig as an
    argument named `klio_config`.

    Be aware this is a function wrapper and must come after any other
    decorators for click options, arguments, etc.
    """

    @options.override
    @options.template
    @options.job_dir
    @options.config_file
    def wrapper(*args, **kwargs):
        raw_overrides = kwargs.pop("override")
        raw_templates = kwargs.pop("template")
        job_dir = kwargs.pop("job_dir")
        config_file = kwargs.pop("config_file")
        job_dir, config_path = get_config_job_dir(job_dir, config_file)

        warn_if_py2_job(job_dir)

        raw_config_data = get_config_by_path(config_path)

        conf = config.KlioConfig(
            raw_config_data,
            raw_templates=raw_templates,
            raw_overrides=raw_overrides,
        )

        meta = KlioConfigMeta(
            job_dir=job_dir, config_file=config_file, config_path=config_path,
        )

        kwargs["klio_config"] = conf
        kwargs["config_meta"] = meta

        func(*args, **kwargs)

    return functools.update_wrapper(wrapper, func)


#############################
# Cli/exec utils
#############################
def warn_if_py2_job(job_dir):
    dockerfile_path = os.path.join(job_dir, "Dockerfile")
    if not os.path.isfile(dockerfile_path):
        return

    from_line = None
    with open(dockerfile_path, "r") as f:
        for line in f.readlines():
            if line.startswith("FROM"):
                from_line = line
                break
    if not from_line:
        # not having a FROM line will break elsewhere, so no need to take
        # care of it here
        return

    py2_dataflow_images = [
        "dataflow.gcr.io/v1beta3/python",
        "dataflow.gcr.io/v1beta3/python-base",
        "dataflow.gcr.io/v1beta3/python-fnapi",
    ]
    from_image = from_line.lstrip("FROM ").split(":")[0]
    if from_image in py2_dataflow_images:
        msg = (
            "Python 2 support in Klio is deprecated. Please upgrade "
            "to Python 3.5+."
        )
        warnings.warn(msg, category=UserWarning)

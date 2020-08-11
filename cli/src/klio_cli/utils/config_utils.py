# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import logging
import warnings

import yaml


SUPPORTED_CONFIG_VERSIONS = (2,)
DEPRECATED_CONFIG_VERSIONS = (1,)
ALL_CONFIG_VERSIONS = SUPPORTED_CONFIG_VERSIONS + DEPRECATED_CONFIG_VERSIONS


def get_config_by_path(config_filepath, parse_yaml=True):
    try:
        with open(config_filepath) as f:
            if parse_yaml:
                return yaml.safe_load(f)
            else:
                return f.read()
    except IOError:
        logging.error("Could not read config file {0}".format(config_filepath))
        raise SystemExit(1)


# TODO: integrate this into KlioConfig as a converter
def set_config_version(config):
    msg_version = config.version
    if msg_version is None:
        logging.info(
            "No value set for 'version' in `klio-job.yaml`. Defaulting to "
            "version 1."
        )
        msg_version = 1

    try:
        msg_version = int(msg_version)

    except ValueError:
        logging.error(
            "Invalid `version` value in `klio-job.yaml`. Expected `int`, "
            "got `{}`".format(type(msg_version))
        )
        raise  # reraises ValueError

    if msg_version not in ALL_CONFIG_VERSIONS:
        logging.error(
            "Unsupported configuration `version` '{}'. Supported versions: "
            "{}".format(msg_version, ALL_CONFIG_VERSIONS)
        )
    if msg_version in DEPRECATED_CONFIG_VERSIONS:
        msg = (
            "Config version {} is deprecated and will be removed in a future "
            "release of klio. Please migrate to a supported "
            "version: {}".format(msg_version, SUPPORTED_CONFIG_VERSIONS)
        )
        logging.warning(msg)
        warnings.warn(msg, DeprecationWarning)

    config.version = msg_version
    return config

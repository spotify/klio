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

import logging
import warnings

SUPPORTED_CONFIG_VERSIONS = (2,)
DEPRECATED_CONFIG_VERSIONS = (1,)
ALL_CONFIG_VERSIONS = SUPPORTED_CONFIG_VERSIONS + DEPRECATED_CONFIG_VERSIONS


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

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

import logging


logger = logging.getLogger("klio")


class _UnsetRequiredValue(object):
    """
    Sentinel class used on as the default for attributes that require a value.
    ``_UnsetRequiredValue`` is a singleton. There is only ever one of it.
    """

    _singleton = None

    def __new__(cls):
        if _UnsetRequiredValue._singleton is None:
            _UnsetRequiredValue._singleton = super(
                _UnsetRequiredValue, cls
            ).__new__(cls)
        return _UnsetRequiredValue._singleton

    def __repr__(self):
        return "UNSET_REQUIRED_VALUE"


UNSET_REQUIRED_VALUE = _UnsetRequiredValue()
"""
This is set as the default for an attribute when we want no default.  This way
attrs itself doesn't raise an exception and instead the value is passed into
our converter which will raise a properly formatted error
"""


class ConfigValueConverter(object):
    """Base class for converters automatically added to any `attrs` `attrib`
    created in a class decorated with `config_object`.  These converters
    properly handle raising Exceptions when a required value is not provided,
    as well as more strict type checking and type coercion.

    """

    def __init__(self, key):
        self.key = key

    def validate(self, value):
        if value == UNSET_REQUIRED_VALUE:
            raise Exception("missing required key: {}".format(self.key))
        elif value is None:
            return value
        return self._validate_value(value)

    def _validate_value(self, value):
        return value


class TypeCheckingConverter(ConfigValueConverter):
    def __init__(self, key, type):
        super().__init__(key)
        self.type = type

    def _wrong_type_msg(self, wrong_value):
        return (
            "{}: expected value of type '{}'" ", got a '{}' value instead"
        ).format(self.key, self.type.__name__, type(wrong_value).__name__)


class SingleValueConverter(TypeCheckingConverter):
    def validate(self, value):
        if isinstance(value, dict) or isinstance(value, list):
            self._wrong_type_message(value)

        return super().validate(value)


class IntConverter(SingleValueConverter):
    def __init__(self, key_prefix):
        super().__init__(key_prefix, int)

    def _validate_value(self, value):
        try:
            return int(value)
        except Exception:
            raise Exception(
                "{}: expected numeric value, got '{}'".format(self.key, value)
            )


class BoolConverter(SingleValueConverter):
    def __init__(self, key_prefix):
        super().__init__(key_prefix, bool)

    def _validate_value(self, value):
        if value is None:
            return value
        try:
            new_value = bool(value)
            if not isinstance(value, bool):
                logger.warning(
                    (
                        "{}: found non-bool value '{}'"
                        ", converting to bool '{}'"
                    ).format(self.key, value, new_value)
                )
            return new_value
        except Exception:
            raise Exception(
                "{}: expected boolean value, got '{}'".format(self.key, value)
            )


class StringConverter(SingleValueConverter):
    def __init__(self, key_prefix):
        super().__init__(key_prefix, str)

    def _validate_value(self, value):
        if value is None:
            return value

        if not isinstance(value, str):
            logger.warning(
                "{}: found non-string value '{}', converting to str".format(
                    self.key, value
                )
            )
        return str(value)


class Converters(object):

    TYPES = {
        int: IntConverter,
        str: StringConverter,
        bool: BoolConverter,
    }

    @classmethod
    def for_type(cls, type, key):
        if type in cls.TYPES:
            return cls.TYPES[type](key).validate
        else:
            return ConfigValueConverter(key).validate

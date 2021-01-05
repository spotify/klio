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

import collections
import string

import glom
import yaml

from klio_core import exceptions


class KlioConfigPreprocessor(object):
    """This handles everything between parsing the config from YAML and passing
    the parsed dict to KlioConfig."""

    # list of lambdas
    PLUGIN_PREPROCESSORS = []

    @classmethod
    def add_plugin_preprocessor(cls, proc):
        """proc should be a function that accepts the config_dict as a
        parameter and returns a modified version of it"""
        cls.PLUGIN_PREPROCESSORS.append(proc)

    @classmethod
    def _apply_plugin_preprocessors(cls, config_dict):
        for pre_processor in cls.PLUGIN_PREPROCESSORS:
            config_dict = pre_processor(config_dict)
        return config_dict

    @staticmethod
    def _transform_io_list(io_subsection_list):
        """Transform lists of dicts into a nested dict of dicts, where the keys
        for the top-level dict come from the `name` field in the nested dict.
        If `name` is not present, a name is auto-generated based on the index
        of the I/O and it's type.

        example:

        [
            {
                "name" : "my cool config",
                "type" : "bigquery",
                ....
            },
            {
                "type" : "file",
                ...
            }
        ]

        is transformed to

        {
            "my cool config" : {
                "name" : "my cool config",
                "type" : "bigquery",
                ...
            },
            "file0" : {
                "type" : "file",
                ...
            },
        }
        """

        type_counters = collections.defaultdict(int)
        io_dict = {}
        for conf in io_subsection_list:
            if "name" in conf:
                name = conf["name"]
                # TODO: right now "name" isn't supported in IOConfig (conflicts
                # with existing "name" attribute), once that's fixed we
                # shouldn't drop name here
                conf.pop("name")
            else:
                type_name = conf.get("type", "unknown")
                type_id = type_counters[type_name]
                type_counters[type_name] += 1
                name = "{}{}".format(type_name, type_id)

            io_dict[name] = conf

        return io_dict

    @classmethod
    def _transform_io_sections(cls, config_dict):
        clone = config_dict.copy()
        for io_type in ["events", "data"]:
            for io_direction in ["inputs", "outputs"]:
                path = "job_config.{}.{}".format(io_type, io_direction)

                glom_assign = glom.Assign(
                    path, glom.Spec((path, cls._transform_io_list))
                )
                glom.glom(
                    clone, glom_assign, default=None,
                )

        return clone

    @staticmethod
    def _apply_templates(raw_config_str, templates):
        """Applies templating to raw klio config.
        Example formats include:
        RAW = '{
            "allow_non_klio_messages": False,
            "events": {
                "inputs": {
                    "bigquery0": {
                        "type": "bigquery",
                        "table": "test.top_tracks_global",
                        "partition": "$YESTERDAY",
                    },
                    "bigquery1": {
                        "type": "bigquery",
                        "table": "test.top_artists_global",
                        "partition": "$TODAY",
                    },
                    "bigquery3": {
                        "type": "bigquery",
                        "table": "testing.${GENRE}_join_$COUNTRY",
                        "partition": "$TODAY",
                    }
                }
            }
        }'

        TEMPLATES = {
            "YESTERDAY": "12-31-2019",
            "TODAY": "01-01-2020",
            "GENRE": "ROCK",
            "COUNTRY": "STO"
        }

        Args:
            raw_config_str (str): raw klio config dict.
            Note mid string templating should be wrapped in ${variable}
            while lone or terminal variables can just be prefixed by a "$"
            templates (dict): template fields to apply to config dict
        Return:
            config_str (str): config dict as string with templates applied
        """
        template = string.Template(raw_config_str)
        try:
            return template.substitute(**templates)
        except KeyError as e:
            raise exceptions.KlioConfigTemplatingException(e)

    @staticmethod
    def _apply_overrides(raw_config, overrides):
        """Applies overrides to raw klio config.
        If a key already exists in the raw config,
        it will be updated with the override value provided in the overrides dict.
        If a key does not yet exist in the raw config,
        it will be created and assigned the override value.
        Example formats include:
            RAW = {
                "allow_non_klio_messages": False,
                "events": {
                    "inputs": {
                        "file0": {
                            "type": "file",
                            "location": "gs://sigint-output/yesterday.txt",
                        },
                        "file1": {
                            "type": "file",
                            "location": "gs://sigint-output/today.txt",
                        }
                    }
                }
            }

            OVER = {
                "allow_non_klio_messages": True,  # Non-nested key
                "events.inputs.file1.location": "gs://sigint-output/01-01-2020.txt",
                "events.inputs.file2.location": "gs://sigint-output/01-02-2020.txt",
                "events.inputs.file2.type": "file"
            }

        Args:
            raw_config (dict): raw klio config dict
            overrides (dict): override field to override value
        Return:
            config (dict): config dict with overrides applied
        """  # NOQA E501

        for path, value in overrides.items():
            glom.assign(raw_config, path, value, missing=dict)
        return raw_config

    @staticmethod
    def _parse_option_key_val_list(key_val_list):
        """Parses a list of strings in the form "<key>=<value>" into a dict of
        the form {"key" : "value"}"""
        key_vals = {}
        for key_val_str in key_val_list:
            # split "key=value" and trim whitespace from both
            pieces = ["".join(p.split()) for p in key_val_str.split("=")]
            if len(pieces) != 2:
                raise Exception(
                    (
                        "Malformed config override '{}',"
                        " should be '<key>=<value>'"
                    ).format(key_val_str)
                )
            key_vals[pieces[0].lower()] = pieces[1]
        return key_vals

    @classmethod
    def _parse_override_list(cls, raw_override_list):
        return cls._parse_option_key_val_list(raw_override_list)

    @classmethod
    def _parse_template_list(cls, raw_template_list):
        return cls._parse_option_key_val_list(raw_template_list)

    @classmethod
    def process(cls, raw_config_data, raw_template_list, raw_override_list):

        template_dict = cls._parse_template_list(raw_template_list)
        override_dict = cls._parse_override_list(raw_override_list)

        if isinstance(raw_config_data, dict):
            raw_config_data = yaml.dump(raw_config_data)

        templated_config_data = cls._apply_templates(
            raw_config_data, template_dict
        )

        config_dict = yaml.safe_load(templated_config_data)

        config_dict = cls._apply_plugin_preprocessors(config_dict)

        transformed_config = cls._transform_io_sections(config_dict)
        override_config = cls._apply_overrides(
            transformed_config, override_dict
        )

        return override_config

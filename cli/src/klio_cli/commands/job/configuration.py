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

import json
import sys

import glom
import yaml

from klio_core import config as kconfig
from klio_core import utils as core_utils
from klio_core.config import _utils as config_utils


class EffectiveJobConfig(object):
    def __init__(self, config_path):
        self.config_path = config_path
        self.config_data = core_utils.get_config_by_path(self.config_path)

    def _get_effective_config(self):
        conf = kconfig.KlioConfig(self.config_data)

        effective_config = conf.as_dict()
        return self._order_config_keys(effective_config)

    @staticmethod
    def _order_config_keys(effective_config):
        key_order = ["version", "job_name", "pipeline_options", "job_config"]
        all_keys = list(effective_config.keys())
        other_top_level_keys = [k for k in all_keys if k not in key_order]
        if other_top_level_keys:
            key_order.extend(other_top_level_keys)
        ordered_effective_config = {}
        for key in key_order:
            ordered_effective_config[key] = effective_config[key]

        return ordered_effective_config

    @staticmethod
    def _sanitize_value(value):
        # sanitize given values to work with yaml dumping - otherwise everything
        # is a string (i.e. streaming: 'True' instead of streaming: true).
        # click argument values will always be strings
        if value.lower() in ("true", "1"):
            value = True
        elif value.lower() in ("false", "0"):
            value = False

        if not isinstance(value, bool):
            try:
                value = int(value)
            except ValueError:
                pass

        return value

    @staticmethod
    def _sanitize_target(target):
        # sanitize section.property value for glom, i.e.
        # from foo.bar[0]baz (jq-like)
        # to foo.bar.0.baz
        if "[" in target and "]" in target:
            target = target.replace("[", ".")
            target = target.replace("]", ".")
        return target

    def _set_config(self, target, value):
        try:
            glom.assign(self.config_data, target, value, missing=dict)
        except glom.mutation.PathAssignError as e:
            if "IndexError" not in str(e):
                raise e
            # handle if user is trying to append to a list - for some reason
            # glom can't handle that
            stems = target.split(".")
            last_index = 0
            for index, stem in enumerate(stems):
                try:
                    int(stem)
                except Exception:
                    continue
                new_target = ".".join(stems[last_index:index])
                property_list = glom.glom(self.config_data, new_target)
                property_list.insert(index, {})

            glom.assign(self.config_data, target, value, missing=dict)

    def show(self):
        effective_config = self._get_effective_config()
        yaml.dump(
            effective_config,
            stream=sys.stdout,
            Dumper=config_utils.IndentListDumper,
            sort_keys=False,
        )

    def set(self, target_to_value):
        for tv in target_to_value:
            target, value = tv.split("=")
            target = self._sanitize_target(target)
            value = self._sanitize_value(value)
            self._set_config(target, value)

        # this will validate the updated config before we write
        effective_config = self._get_effective_config()

        with open(self.config_path, "w") as f:
            yaml.dump(
                effective_config,
                stream=f,
                Dumper=config_utils.IndentListDumper,
                default_flow_style=False,
                sort_keys=False,
            )

    def unset(self, target):
        target = self._sanitize_target(target)

        glom.delete(self.config_data, target, ignore_missing=True)

        with open(self.config_path, "w") as f:
            yaml.dump(
                self.config_data,
                stream=f,
                Dumper=config_utils.IndentListDumper,
                default_flow_style=False,
                sort_keys=False,
            )

    def get(self, target):
        target = self._sanitize_target(target)
        effective_config = self._get_effective_config()
        value = glom.glom(effective_config, target)
        print(json.dumps(value, indent=2))

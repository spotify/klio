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

import inspect

import attr
import pkg_resources

from py import io


@attr.s
class KlioPlugin(object):
    plugin_name = attr.ib(type=str)
    description = attr.ib(type=str)
    package_name = attr.ib(type=str)
    package_version = attr.ib(type=str)
    module_path = attr.ib(type=str)


# TODO: in the future, add functionality to toggle & configure audit
#       steps in a job's klio-job.yaml file (@lynn)
def load_plugins_by_namespace(namespace):
    """Loads audit steps defined in `setup.py` under a given namespace.

    Args:
        namespace (str): namespace under which to look for plugins.
    Returns:
        Loaded plugin objects (list).
    """
    return [e.load() for e in pkg_resources.iter_entry_points(namespace)]


def _get_plugins_by_namespace(namespace):
    entrypoints = pkg_resources.iter_entry_points(namespace)
    for ep in entrypoints:
        # Need to actually load the plugin in order to get its location,
        # as well as class attributes, like name & description
        loaded = ep.load()

        desc = loaded.get_description() or loaded.__doc__
        if desc is None:
            desc = "No description."

        yield KlioPlugin(
            plugin_name=loaded.AUDIT_STEP_NAME,
            description=desc,
            package_name=ep.dist.project_name,
            package_version=ep.dist.parsed_version,
            module_path=inspect.getfile(loaded),
        )


def print_plugins(namespace, tw=None):
    plugin_meta = (
        " -- via {package_name} (v{package_version}) -- {module_path}\n"
    )
    plugin_desc = "\t{desc}\n\n"

    if not tw:
        tw = io.TerminalWriter()

    loaded_plugins = _get_plugins_by_namespace(namespace)
    for plugin in loaded_plugins:
        meta = plugin_meta.format(
            package_name=plugin.package_name,
            package_version=plugin.package_version,
            module_path=plugin.module_path,
        )
        tw.write(plugin.plugin_name, blue=True, bold=True)
        tw.write(meta, green=True)
        tw.write(plugin_desc.format(desc=plugin.description))

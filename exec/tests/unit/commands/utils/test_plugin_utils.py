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

import pytest

from klio_exec.commands.utils import plugin_utils


@pytest.fixture
def mock_inspect(mocker, monkeypatch):
    mock = mocker.Mock()
    mock.getfile.return_value = "/path/to/package"
    monkeypatch.setattr(plugin_utils, "inspect", mock)
    return mock


@pytest.fixture
def mock_plugins(mocker):
    mock_dist = mocker.Mock()
    mock_dist.project_name = "package-name"
    mock_dist.parsed_version = "1.2.3"

    mock_plugins = []
    for i in range(2):
        mock_loaded = mocker.Mock()
        mock_loaded.AUDIT_STEP_NAME = "step-{}".format(i)

        mock_plugin = mocker.Mock(name=i)
        mock_plugin.load.return_value = mock_loaded
        mock_plugin.dist = mock_dist
        mock_plugins.append(mock_plugin)
    return mock_plugins


@pytest.fixture
def mock_iter_entry_points(mock_plugins, mocker, monkeypatch):
    mock = mocker.Mock()
    mock.return_value = mock_plugins
    monkeypatch.setattr(plugin_utils.pkg_resources, "iter_entry_points", mock)
    return mock


def test_load_plugins_by_namespace(mock_plugins, mock_iter_entry_points):
    ret = plugin_utils.load_plugins_by_namespace("a.namespace")

    mock_iter_entry_points.assert_called_once_with("a.namespace")
    for plugin in mock_plugins:
        plugin.load.assert_called_once_with()

    assert ret == [p.load.return_value for p in mock_plugins]


@pytest.mark.parametrize(
    "get_desc,docstr,exp_desc",
    (
        ("a description", None, "a description"),
        (None, "a docstring desc", "a docstring desc"),
        (None, None, "No description."),
        ("a description", "a docstring desc", "a description"),
    ),
)
def test_get_plugins_by_namespace(
    get_desc,
    docstr,
    exp_desc,
    mock_inspect,
    mock_plugins,
    mock_iter_entry_points,
    mocker,
):
    for mp in mock_plugins:
        mp.load.return_value.get_description.return_value = get_desc
        mp.load.return_value.__doc__ = docstr

    exp_plugins = []
    for i in range(2):
        exp_p = plugin_utils.KlioPlugin(
            plugin_name="step-{}".format(i),
            description=exp_desc,
            package_name="package-name",
            package_version="1.2.3",
            module_path="/path/to/package",
        )
        exp_plugins.append(exp_p)

    actual_plugins = list(
        plugin_utils._get_plugins_by_namespace("a.namespace")
    )

    mock_iter_entry_points.assert_called_once_with("a.namespace")
    for plugin in mock_plugins:
        plugin.load.assert_called_once_with()
        exp_calls = [mocker.call(plugin.load.return_value)]
        mock_inspect.getfile.assert_has_calls(exp_calls)

    assert len(mock_plugins) == mock_inspect.getfile.call_count
    assert exp_plugins == actual_plugins


@pytest.mark.parametrize("tw", (True, False))
def test_print_plugins(tw, mock_terminal_writer, mocker, monkeypatch):
    loaded_plugins = [
        plugin_utils.KlioPlugin(
            plugin_name="plugin_name",
            description="a desc",
            package_name="package-name",
            package_version="1.2.3",
            module_path="/path/to/package",
        )
    ]

    mock_get_plugins_by_ns = mocker.Mock()
    mock_get_plugins_by_ns.return_value = loaded_plugins
    monkeypatch.setattr(
        plugin_utils, "_get_plugins_by_namespace", mock_get_plugins_by_ns
    )

    exp_plugin_meta_str = " -- via package-name (v1.2.3) -- /path/to/package\n"
    exp_plugin_desc = "\ta desc\n\n"
    exp_calls = [
        mocker.call("plugin_name", blue=True, bold=True),
        mocker.call(exp_plugin_meta_str, green=True),
        mocker.call(exp_plugin_desc),
    ]

    kwargs = {"tw": None}
    if tw:
        kwargs["tw"] = mock_terminal_writer

    plugin_utils.print_plugins("a.namespace", **kwargs)

    mock_get_plugins_by_ns.assert_called_once_with("a.namespace")
    mock_terminal_writer.write.assert_has_calls(exp_calls)
    assert 3 == mock_terminal_writer.write.call_count

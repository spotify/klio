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

from klio_cli.utils import config_utils


def test_get_config_by_path(mocker, monkeypatch):
    m_open = mocker.mock_open()
    mock_open = mocker.patch("klio_cli.utils.config_utils.open", m_open)

    mock_safe_load = mocker.Mock()
    monkeypatch.setattr(config_utils.yaml, "safe_load", mock_safe_load)

    path = "path/to/a/file"
    config_utils.get_config_by_path(path)

    mock_open.assert_called_once_with(path)


def test_get_config_by_path_error(mocker, monkeypatch, caplog):
    m_open = mocker.mock_open()
    mock_open = mocker.patch("klio_cli.utils.config_utils.open", m_open)
    mock_open.side_effect = IOError

    with pytest.raises(SystemExit):
        path = "path/to/a/file"
        config_utils.get_config_by_path(path)

    assert 1 == len(caplog.records)

from __future__ import absolute_import

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

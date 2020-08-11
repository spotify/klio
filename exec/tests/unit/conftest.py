# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import logging

import py
import pytest


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def mock_terminal_writer(mocker, monkeypatch):
    mock_tw = mocker.Mock()
    monkeypatch.setattr(py.io, "TerminalWriter", mock_tw)
    return mock_tw.return_value

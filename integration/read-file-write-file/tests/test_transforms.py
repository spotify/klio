# Copyright 2020 Spotify AB

import json
import logging
import os

import pytest

from klio_core.proto import klio_pb2

import transforms


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def mock_item(mocker):
    return mocker.Mock()


@pytest.fixture
def expected_log_messages(mock_item):
    return [
        "Hello, Klio!",
        "Received element {}".format(mock_item.element),
        "Received payload {}".format(mock_item.payload),
    ]


def test_process(mock_item, expected_log_messages, caplog):
    helloklio_fn = transforms.LogKlioMessage()
    output = helloklio_fn.process(mock_item)
    assert mock_item == list(output)[0]

    for index, record in enumerate(caplog.records):
        assert "INFO" == record.levelname
        assert expected_log_messages[index] == record.message


def test_input_exists(mock_item):
    helloklio_fn = transforms.LogKlioMessage()
    assert helloklio_fn.input_data_exists(mock_item.element) is True


def test_output_exists(mock_item):
    helloklio_fn = transforms.LogKlioMessage()
    assert helloklio_fn.output_data_exists(mock_item.element) is False

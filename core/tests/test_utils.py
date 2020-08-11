# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import pytest

from google.api_core import exceptions as gapi_exceptions

from klio_core import utils


def test_set_global():
    utils.set_global("set-a-value", "a-value")

    actual = getattr(utils, "klio_global_state_set-a-value", None)
    assert "a-value" == actual

    delattr(utils, "klio_global_state_set-a-value")


def test_get_global():
    setattr(utils, "klio_global_state_get-a-value", "a-value")

    actual = utils.get_global("get-a-value")
    assert "a-value" == actual

    delattr(utils, "klio_global_state_get-a-value")


@pytest.mark.parametrize("exists", (True, False))
def test_delete_global(exists):
    if exists:
        setattr(utils, "klio_global_state_delete-a-value", "a-value")

    utils.delete_global("delete-a-value")

    actual = getattr(utils, "klio_global_state_delete-a-value", None)

    assert not actual


@pytest.mark.parametrize(
    "set_value,callable_init", ((True, False), (False, True), (False, False))
)
def test_get_or_initialize_global(set_value, callable_init, mocker):
    if set_value:
        setattr(utils, "klio_global_state_get-or-init-value", "a-value")
    if callable_init:
        initializer = mocker.Mock(return_value="a-value")
    else:
        initializer = "a-value"

    actual = utils.get_or_initialize_global("get-or-init-value", initializer)

    assert "a-value" == actual
    if not isinstance(initializer, str):
        initializer.assert_called_once_with()

    delattr(utils, "klio_global_state_get-or-init-value")


@pytest.fixture
def mock_publisher(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(utils.pubsub, "PublisherClient", mock)
    return mock


def test_private_get_publisher(mock_publisher):
    ret_publisher = utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    mock_publisher.return_value.create_topic.assert_called_once_with("a-topic")

    assert mock_publisher.return_value == ret_publisher


def test_private_get_publisher_topic_exists(mock_publisher):
    client = mock_publisher.return_value
    client.create_topic.side_effect = gapi_exceptions.AlreadyExists("foo")

    ret_publisher = utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.create_topic.assert_called_once_with("a-topic")

    assert client == ret_publisher


def test_private_get_publisher_raises(mock_publisher):
    client = mock_publisher.return_value
    client.create_topic.side_effect = Exception("foo")

    with pytest.raises(Exception, match="foo"):
        utils._get_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.create_topic.assert_called_once_with("a-topic")


@pytest.mark.parametrize("in_globals", (True, False))
def test_get_publisher(in_globals, mock_publisher):
    client = mock_publisher.return_value
    if in_globals:
        setattr(utils, "klio_global_state_publisher_a-topic", client)

    ret_publisher = utils.get_publisher("a-topic")

    assert client == ret_publisher
    delattr(utils, "klio_global_state_publisher_a-topic")

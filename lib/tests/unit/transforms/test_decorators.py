# Copyright 2020 Spotify AB

import pytest

from klio_core.proto import klio_pb2

from klio.transforms import decorators


@pytest.fixture
def kmsg():
    msg = klio_pb2.KlioMessage()
    msg.data.element = b"3l3m3nt"
    return msg


def test_retry(kmsg, mocker, mock_config):
    mock_function = mocker.Mock()

    @decorators._handle_klio
    @decorators._retry(tries=2)
    def func(*args, **kwargs):
        mock_function(*args, **kwargs)
        raise Exception("fuu")

    func(kmsg.SerializeToString())

    assert 2 == mock_function.call_count


def test_retry_custom_catch(kmsg, mocker, mock_config):

    # Assert retry on custom exception
    class CustomCatchException(Exception):
        pass

    mock_function = mocker.Mock()
    raise_custom = True

    @decorators._handle_klio
    @decorators._retry(tries=3, exception=CustomCatchException)
    def func(*args, **kwargs):
        mock_function(*args, **kwargs)
        if raise_custom:
            raise CustomCatchException("custom fuu")
        raise Exception("fuu")

    func(kmsg.SerializeToString())
    assert 3 == mock_function.call_count

    # Assert retry on multiple custom exceptions
    class AnotherCustomCatchException(Exception):
        pass

    exc_tuple = (CustomCatchException, AnotherCustomCatchException)

    @decorators._handle_klio
    @decorators._retry(tries=3, exception=exc_tuple)
    def func(*args, **kwargs):
        mock_function(*args, **kwargs)
        if raise_custom:
            raise CustomCatchException("custom fuu")
        raise Exception("fuu")

    mock_function.reset_mock()
    func(kmsg.SerializeToString())
    assert 3 == mock_function.call_count

    # Assert retries do not happen for an exception that isn't provided
    mock_function.reset_mock()
    raise_custom = False
    func(kmsg.SerializeToString())
    assert 1 == mock_function.call_count


def test_retry_raises_runtime_parents(kmsg, mocker, mock_config):
    # Need to call @retry with parens
    with pytest.raises(RuntimeError):

        @decorators._handle_klio
        @decorators.retry
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())


@pytest.mark.parametrize(
    "invalid_tries", (-2, 0.5, "1", {"a": "dict"}, ["a", "list"], lambda x: x)
)
def test_retry_raises_runtime_invalid_tries(
    invalid_tries, kmsg, mocker, mock_config
):
    # Assert `tries` as a valid integer
    with pytest.raises(RuntimeError):

        @decorators._handle_klio
        @decorators._retry(tries=invalid_tries)
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())


@pytest.mark.parametrize(
    "invalid_delay", (-2, {"a": "dict"}, ["a", "list"], lambda x: x)
)
def test_retry_raises_runtime_invalid_delay(
    invalid_delay, kmsg, mocker, mock_config
):
    # Assert `delay` as a valid int/float
    with pytest.raises(RuntimeError):

        @decorators._handle_klio
        @decorators._retry(tries=1, delay=invalid_delay)
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())

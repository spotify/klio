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

from klio_core.proto import klio_pb2

from klio.transforms import decorators
from klio.utils import _thread_limiter


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


@pytest.mark.filterwarnings(
    (
        "ignore:'retry' is experimental and is subject to incompatible "
        "changes, or removal in a future release of Klio."
    )
)
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


@pytest.mark.parametrize(
    "max_thread_count,patch_str",
    (
        (None, "threading.BoundedSemaphore"),
        (_thread_limiter.ThreadLimit.DEFAULT, "threading.BoundedSemaphore"),
        (_thread_limiter.ThreadLimit.NONE, "_DummySemaphore"),
    ),
)
def test_thread_limiting(
    max_thread_count, patch_str, kmsg, mock_config, mocker, monkeypatch
):
    mock_function = mocker.Mock()
    mock_semaphore = mocker.Mock()
    monkeypatch.setattr(
        f"klio.utils._thread_limiter.{patch_str}", mock_semaphore
    )

    kwargs = {}
    if max_thread_count is not None:
        kwargs["max_thread_count"] = max_thread_count

    @decorators._handle_klio(**kwargs)
    def func(*args, **kwargs):
        mock_function(*args, **kwargs)
        return

    func(kmsg.SerializeToString())

    assert 1 == mock_function.call_count
    mock_semaphore.return_value.acquire.assert_called_once_with()
    mock_semaphore.return_value.release.assert_called_once_with()


def test_thread_limiting_custom_limiter(
    kmsg, mock_config, mocker, monkeypatch
):
    mock_function = mocker.Mock()
    mock_semaphore = mocker.Mock()

    limiter = _thread_limiter.ThreadLimiter(max_thread_count=1)
    monkeypatch.setattr(limiter, "_semaphore", mock_semaphore)

    @decorators._handle_klio(thread_limiter=limiter)
    def func(*args, **kwargs):
        mock_function(*args, **kwargs)
        return

    func(kmsg.SerializeToString())

    assert 1 == mock_function.call_count
    mock_semaphore.acquire.assert_called_once_with()
    mock_semaphore.release.assert_called_once_with()


def test_thread_limiting_raises_mutex_args(kmsg, mocker, mock_config):
    limiter = _thread_limiter.ThreadLimiter(max_thread_count=1)

    with pytest.raises(RuntimeError):

        @decorators._handle_klio(max_thread_count=1, thread_limiter=limiter)
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())


def test_thread_limiting_raises_invalid_limiter(kmsg, mocker, mock_config):
    limiter = "not an instance of ThreadLimiter"

    with pytest.raises(RuntimeError):

        @decorators._handle_klio(thread_limiter=limiter)
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())


@pytest.mark.parametrize(
    "invalid_max_thread_count", (-2, {"a": "dict"}, ["a", "list"])
)
def test_thread_limiting_raises_invalid_max(
    invalid_max_thread_count, kmsg, mocker, mock_config
):

    with pytest.raises(RuntimeError):

        @decorators._handle_klio(max_thread_count=invalid_max_thread_count)
        def func(*args, **kwargs):
            pass

        func(kmsg.SerializeToString())

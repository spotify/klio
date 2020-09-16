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

from klio_exec.commands.utils import wrappers


class DummyTransformGenerator(object):
    def process(self, x):
        yield x + 10
        yield x + 20


class DummyTransformGeneratorRaises(object):
    def process(self, x):
        yield x + 10
        yield x + 20
        raise Exception("catch me")


class DummyTransformFunc(object):
    def process(self, x):
        return x + 10


class DummyTransformFuncRaises(object):
    def process(self, *args):
        raise Exception("catch me")


class TestProfiler(wrappers.KLineProfilerMixin):
    def add_function(self, *args, **kwargs):
        pass

    def enable_by_count(self, *args, **kwargs):
        pass

    def disable_by_count(self, *args, **kwargs):
        pass


@pytest.fixture
def k_profiler():
    return TestProfiler()


@pytest.fixture
def mock_add_func(k_profiler, mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(k_profiler, "add_function", mock)
    return mock


@pytest.fixture
def mock_enable_by_count(k_profiler, mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(k_profiler, "enable_by_count", mock)
    return mock


@pytest.fixture
def mock_disable_by_count(k_profiler, mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(k_profiler, "disable_by_count", mock)
    return mock


def dummy_func(x):
    return x + 10


def dummy_gen(x):
    yield x + 10
    yield x + 20


@pytest.mark.parametrize(
    "transform,print_msg",
    (
        (DummyTransformGenerator, ""),
        (DummyTransformGeneratorRaises, "WARN: Error caught while profiling"),
    ),
)
def test_print_user_exceptions_generators(transform, print_msg, capsys):
    transform, *_ = list(wrappers.print_user_exceptions([transform]))

    # fails without functools.wraps(func)
    assert "process" == transform.process.__name__

    # first argument is "self" of the process method
    result = transform.process(None, 10)

    assert 20 == next(result)
    assert 30 == next(result)
    with pytest.raises(StopIteration):
        next(result)

    captured = capsys.readouterr()
    assert print_msg in captured.out


@pytest.mark.parametrize(
    "transform,print_msg,exp_ret",
    (
        (DummyTransformFunc, "", 20),
        (DummyTransformFuncRaises, "WARN: Error caught while profiling", None),
    ),
)
def test_print_user_exceptions_funcs(transform, print_msg, exp_ret, capsys):
    transform, *_ = list(wrappers.print_user_exceptions([transform]))

    # fails without functools.wraps(func)
    assert "process" == transform.process.__name__

    # first argument is "self" of the process method
    result = transform.process(None, 10)

    assert exp_ret == result
    captured = capsys.readouterr()
    assert print_msg in captured.out


def test_call(
    k_profiler, mock_add_func, mock_enable_by_count, mock_disable_by_count
):
    wrapper = k_profiler.__call__(dummy_func)

    mock_add_func.assert_called_once_with(dummy_func)
    mock_enable_by_count.assert_not_called()
    mock_disable_by_count.assert_not_called()
    assert "dummy_func" == wrapper.__name__

    result = wrapper(10)
    assert 20 == result
    mock_enable_by_count.assert_called_once_with()
    mock_disable_by_count.assert_called_once_with()

    mock_add_func.reset_mock()
    mock_enable_by_count.reset_mock()
    mock_disable_by_count.reset_mock()

    wrapper = k_profiler.__call__(dummy_gen)

    mock_add_func.assert_called_once_with(dummy_gen)
    mock_enable_by_count.assert_not_called()
    mock_disable_by_count.assert_not_called()
    assert "dummy_gen" == wrapper.__name__

    result = wrapper(10)

    assert 20 == next(result)
    assert 30 == next(result)
    with pytest.raises(StopIteration):
        next(result)

    mock_enable_by_count.assert_called_once_with()
    mock_disable_by_count.assert_called_once_with()

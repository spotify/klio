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

from klio_exec.commands.utils import memory_utils


@pytest.fixture
def k_profiler():
    return memory_utils.KMemoryLineProfiler()


@pytest.fixture
def mock_show_results(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(memory_utils.mp, "show_results", mock)
    return mock


def dummy_func(x):
    return x + 10


def dummy_gen(x):
    yield x + 10
    yield x + 20


def test_wrap_per_element(k_profiler, mock_show_results, mocker, monkeypatch):
    # don't want to mock the whole class since we need to test a class method,
    # just mocking out the creation of a new instance to return what we can
    # control
    mock_new = mocker.Mock(return_value=k_profiler)
    monkeypatch.setattr(memory_utils.KMemoryLineProfiler, "__new__", mock_new)

    wrapper = memory_utils.KMemoryLineProfiler.wrap_per_element(dummy_func)

    mock_new.assert_not_called()
    mock_show_results.assert_not_called()
    assert "dummy_func" == wrapper.__name__

    result = wrapper(10)
    assert 20 == result
    mock_new.assert_called_once_with(
        memory_utils.KMemoryLineProfiler, backend="psutil"
    )
    mock_show_results.assert_called_once_with(k_profiler, stream=None)

    mock_new.reset_mock()
    mock_show_results.reset_mock()

    wrapper = memory_utils.KMemoryLineProfiler.wrap_per_element(dummy_gen)

    mock_new.assert_not_called()
    mock_show_results.assert_not_called()
    assert "dummy_gen" == wrapper.__name__

    result = wrapper(10)
    assert 20 == next(result)
    assert 30 == next(result)
    with pytest.raises(StopIteration):
        next(result)
    mock_new.assert_called_once_with(
        memory_utils.KMemoryLineProfiler, backend="psutil"
    )
    mock_show_results.assert_called_once_with(k_profiler, stream=None)


def test_wrap_maximum(k_profiler, mocker, monkeypatch):
    wrapper = memory_utils.KMemoryLineProfiler.wrap_maximum(
        k_profiler, dummy_func
    )

    assert "dummy_func" == wrapper.__name__

    result = wrapper(10)
    assert 20 == result

    wrapper = memory_utils.KMemoryLineProfiler.wrap_maximum(
        k_profiler, dummy_gen
    )

    assert "dummy_gen" == wrapper.__name__

    result = wrapper(10)
    assert 20 == next(result)
    assert 30 == next(result)
    with pytest.raises(StopIteration):
        next(result)

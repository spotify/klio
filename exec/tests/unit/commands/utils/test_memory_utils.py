# Copyright 2020 Spotify AB

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

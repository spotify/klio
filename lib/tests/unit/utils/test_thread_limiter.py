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

import multiprocessing

import pytest

from klio.utils import _thread_limiter


N_CPU = multiprocessing.cpu_count()
BASE_LOG = "Initial semaphore value:"


@pytest.mark.parametrize(
    "max_thread_count, exp_count_log",
    (
        (1, f"{BASE_LOG} 1"),
        (_thread_limiter.ThreadLimit.NONE, "Using unlimited semaphore"),
        (_thread_limiter.ThreadLimit.DEFAULT, f"{BASE_LOG} {N_CPU}"),
        (lambda: 2 * 3, f"{BASE_LOG} 6"),
    ),
)
def test_thread_limiter(max_thread_count, exp_count_log, caplog):
    _thread_limiter.ThreadLimiter(max_thread_count=max_thread_count)

    assert 1 == len(caplog.messages)
    assert exp_count_log in caplog.messages[0]


@pytest.mark.parametrize(
    "max_thread_count", (2, _thread_limiter.ThreadLimit.NONE)
)
def test_thread_limiter_ctx_mgr(max_thread_count, mocker, monkeypatch, caplog):
    mock_semaphore = mocker.Mock()
    limiter = _thread_limiter.ThreadLimiter(max_thread_count=max_thread_count)
    monkeypatch.setattr(limiter, "_semaphore", mock_semaphore)

    with limiter:
        3 * 3

    mock_semaphore.acquire.assert_called_once_with()
    mock_semaphore.release.assert_called_once_with()
    assert 3 == len(caplog.messages)

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

import numpy as np
import pytest

from klio_exec.commands.utils import profile_utils


def test_get_profiling_data(mocker):
    profiling_data = "MEM 100 100001\n" "MEM 200 100002\n" "MEM 300 100003\n"
    m_open = mocker.mock_open(read_data=profiling_data)
    mock_open = mocker.patch(
        "klio_exec.commands.utils.profile_utils.open", m_open
    )

    act_ret = profile_utils._get_profiling_data("foo.txt")

    mock_open.assert_called_once_with("foo.txt", "r")
    exp_ret = {
        "data": [100.0, 200.0, 300.0],
        "timestamp": [100001.0, 100002.0, 100003.0],
    }
    assert exp_ret == act_ret


def test_get_profiling_data_raises_no_file(mocker, caplog):
    with pytest.raises(SystemExit):
        ret = profile_utils._get_profiling_data("i_should_not_exist.txt")
        assert ret is None

    assert 1 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname
    assert "Could not read profiling data" in caplog.records[0].message


def test_get_profiling_data_malformed_data(mocker):
    profiling_data = (
        "MEM 100 100001\n" "foobar baz\n" "\n" "MEM 1 2 3\n" "MEM 300 100003\n"
    )
    m_open = mocker.mock_open(read_data=profiling_data)
    mock_open = mocker.patch(
        "klio_exec.commands.utils.profile_utils.open", m_open
    )

    act_ret = profile_utils._get_profiling_data("foo.txt")

    mock_open.assert_called_once_with("foo.txt", "r")
    exp_ret = {"data": [100.0, 300.0], "timestamp": [100001.0, 100003.0]}
    assert exp_ret == act_ret


def test_get_profiling_data_raises_no_data(mocker, caplog):
    m_open = mocker.mock_open(read_data="\n")
    mock_open = mocker.patch(
        "klio_exec.commands.utils.profile_utils.open", m_open
    )

    with pytest.raises(SystemExit):
        ret = profile_utils._get_profiling_data("foo.txt")
        assert ret is None

    mock_open.assert_called_once_with("foo.txt", "r")
    assert 1 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname
    assert "No samples to parse in" in caplog.records[0].message


def test_plot(mocker, monkeypatch):
    parsed_data = {
        "data": [100.0, 200.0, 300.0],
        "timestamp": [100001.0, 100002.0, 100003.0],
    }
    mock_get_profiling_data = mocker.Mock()
    mock_get_profiling_data.return_value = parsed_data
    monkeypatch.setattr(
        profile_utils, "_get_profiling_data", mock_get_profiling_data
    )
    mock_plt = mocker.Mock()
    monkeypatch.setattr(profile_utils, "plt", mock_plt)

    profile_utils.plot(
        "input.txt", "output.png", "x-label", "y-label", "title test"
    )

    mock_get_profiling_data.assert_called_once_with("input.txt")
    mock_plt.figure.assert_called_once_with(figsize=(14, 6), dpi=90)

    # can't directly compare (or assert called with) np arrays;
    # grabbing the args/kwargs then using np.allclose with a 0 tolerance
    exp_data = np.asarray(parsed_data["data"])
    ts = np.asarray(parsed_data["timestamp"])
    exp_t = ts - float(ts[0])
    act_call_args, act_call_kwargs = mock_plt.plot.call_args
    assert 3 == len(act_call_args)
    assert not act_call_kwargs
    assert np.allclose(exp_t, act_call_args[0], rtol=0)
    assert np.allclose(exp_data, act_call_args[1], rtol=0)
    assert "+-c" == act_call_args[2]

    mock_plt.xlabel.assert_called_once_with("x-label")
    mock_plt.ylabel.assert_called_once_with("y-label")
    mock_plt.title.assert_called_once_with("title test")
    mock_plt.grid.assert_called_once_with()
    mock_plt.savefig.assert_called_once_with("output.png")

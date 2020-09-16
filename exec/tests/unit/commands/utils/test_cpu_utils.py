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

from klio_exec.commands.utils import cpu_utils


@pytest.mark.parametrize("interval,exp_sleep", ((None, 0.1), (0.5, 0.5)))
def test_get_cpu_usage(interval, exp_sleep, mocker, monkeypatch, capsys):
    mock_proc = mocker.Mock()
    # wanting at least 50 lines to hit the flush
    proc_side_effect = [None] * 51
    proc_side_effect.append(True)
    mock_proc.poll.side_effect = proc_side_effect

    mock_time = mocker.Mock()
    timestamps = range(1, 53)
    mock_time.time.side_effect = timestamps
    monkeypatch.setattr(cpu_utils, "time", mock_time)

    cpu_measurements = (29.0, 51.4, 78.1, 61.2) * 13
    mock_cpu_percent = mocker.Mock(side_effect=cpu_measurements)
    monkeypatch.setattr(cpu_utils.psutil, "cpu_percent", mock_cpu_percent)

    cpu_utils.get_cpu_usage(mock_proc, interval=interval)

    assert 52 == mock_time.sleep.call_count
    assert mocker.call(exp_sleep) == mock_time.sleep.call_args

    captured = capsys.readouterr()

    data = zip(cpu_measurements, timestamps)
    exp_out = "\n".join("CPU {} {}.0000".format(d[0], d[1]) for d in data)
    exp_out = exp_out + "\n"
    assert exp_out == captured.out

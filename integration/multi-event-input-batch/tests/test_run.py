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

import logging

import pytest

from klio_core.proto import klio_pb2

import run


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def klio_msg():
    element = b"s0m3_tr4ck_1d"
    msg = klio_pb2.KlioMessage()
    msg.data.element = element
    msg.version = klio_pb2.Version.V2
    return msg

@pytest.mark.parametrize(
    "func_to_test,exp_log_prefix",
    (
        (run.first_func, "[first_func]:"),
        (run.second_func, "[second_func]:"),
        (run.combined_func, "[combined_func]:"),
    )
)
def test_process_funcs(func_to_test, exp_log_prefix, klio_msg, caplog):
    ret = func_to_test(klio_msg.SerializeToString())

    assert klio_msg.SerializeToString() == ret
    assert 1 == len(caplog.records)
    exp_log_msg = f"{exp_log_prefix} {klio_msg.data.element}"
    assert exp_log_msg == caplog.records[0].message
    assert "INFO" == caplog.records[0].levelname

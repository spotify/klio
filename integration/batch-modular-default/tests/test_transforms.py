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

import transforms


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


@pytest.fixture
def expected_log_messages(klio_msg):
    return [
        (
            "KlioThreadLimiter(name=LogKlioMessage.process) Blocked – "
            "waiting on semaphore for an available thread (available threads:"
        ),
        (
            "KlioThreadLimiter(name=LogKlioMessage.process) Released "
            "semaphore (available threads:"
        ),
        "Received element {}".format(klio_msg.data.element),
        "Received payload {}".format(klio_msg.data.payload),
    ]


def test_process(klio_msg, expected_log_messages, caplog):
    helloklio_fn = transforms.LogKlioMessage()
    output = helloklio_fn.process(klio_msg.SerializeToString())

    assert klio_msg.SerializeToString() == list(output)[0]

    assert len(caplog.records) == len(expected_log_messages)

    for index, record in enumerate(caplog.records):
        assert "INFO" == record.levelname
        assert expected_log_messages[index] in record.message

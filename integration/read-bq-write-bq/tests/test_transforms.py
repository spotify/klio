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

import json
import logging
import time

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
        {
            "level": "DEBUG",
            "message": (
                "KlioThreadLimiter(name=LogKlioMessage.process) Blocked – "
                "waiting on semaphore for an available thread (available threads:"
            ),
        },
        {
            "level": "DEBUG",
            "message": (
                "[kmsg-received] value: 1 transform: 'LogKlioMessage.process' "
                "tags: {'metric_type': 'counter'}"
            )
        },
        {"level": "INFO", "message": "Hello, Klio!"},
        {
            "level": "INFO",
            "message": "Received element {}".format(klio_msg.data.element),
        },
        {
            "level": "INFO",
            "message": "Received payload {}".format(klio_msg.data.payload),
        },
        {
            "level": "DEBUG",
            "message": (
                "KlioThreadLimiter(name=LogKlioMessage.process) Released "
                "semaphore (available threads:"
            ),
        },
        {
            "level": "DEBUG",
            "message": (
                "[kmsg-success] value: 1 transform: 'LogKlioMessage.process' "
                "tags: {'metric_type': 'counter'}"
            )
        },
        {
            "level": "DEBUG",
            "message": "[kmsg-timer] value:",
        },
    ]


def test_process(klio_msg, expected_log_messages, caplog):
    helloklio_fn = transforms.LogKlioMessage()
    output = helloklio_fn.process(klio_msg.SerializeToString())

    row = {
        "entity_id": klio_msg.data.element.decode("utf-8"),
        "value": klio_msg.data.element.decode("utf-8")
    }
    expected_kmsg = klio_pb2.KlioMessage()
    expected_kmsg.data.element = klio_msg.data.element
    expected_kmsg.data.payload = bytes(json.dumps(row), "utf-8")
    expected_kmsg.version = klio_pb2.Version.V2

    assert expected_kmsg.SerializeToString() == list(output)[0]

    # logs may not all be available yet since some may be on a different thread
    # so we'll wait a second
    time.sleep(1)
    assert len(caplog.records) == len(expected_log_messages)

    for index, record in enumerate(caplog.records):
        expected_log_message = expected_log_messages[index]
        assert expected_log_message["level"] == record.levelname
        assert expected_log_message["message"] in record.message

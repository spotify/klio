# Copyright 2019-2020 Spotify AB
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

from apache_beam import pvalue
from google.protobuf import message as gproto_message

from klio_core.proto.v1beta1 import klio_pb2

from klio.message import exceptions
from klio.message import serializer


def _get_klio_job():
    job = klio_pb2.KlioJob()
    job.job_name = "klio-job"
    job.gcp_project = "test-project"
    return job


def _get_klio_message():
    parent_klio_job = _get_klio_job()
    msg = klio_pb2.KlioMessage()
    msg.metadata.visited.extend([parent_klio_job])
    msg.metadata.force = True
    msg.metadata.ping = True
    msg.data.element = b"1234567890"
    msg.version = klio_pb2.Version.V2

    return msg


@pytest.fixture
def klio_message():
    return _get_klio_message()


@pytest.fixture
def klio_message_str(klio_message):
    return klio_message.SerializeToString()


@pytest.fixture
def logger(mocker):
    return mocker.Mock()


@pytest.mark.parametrize(
    "version",
    (klio_pb2.Version.UNKNOWN, klio_pb2.Version.V1, klio_pb2.Version.V2),
)
@pytest.mark.parametrize(
    "element,entity_id,payload",
    (
        (b"an-element", None, None),
        (None, "an-entity-id", None),
        (None, "an-entity-id", b"some-payload"),
        (b"an-element", None, b"some-payload"),
        (None, None, b"some-payload"),
    ),
)
def test_handle_msg_compat(version, element, entity_id, payload):
    msg = klio_pb2.KlioMessage()
    msg.version = version
    if element:
        msg.data.element = element
    if payload:
        msg.data.payload = payload
    if entity_id:
        msg.data.entity_id = entity_id

    actual_msg = serializer._handle_msg_compat(msg)
    assert actual_msg.version is not klio_pb2.Version.UNKNOWN
    # we assume in the function's logic that v2 messages are already parsed
    # correctly
    if entity_id and not klio_pb2.Version.V2:
        assert entity_id == actual_msg.data.element.decode("utf-8")


def test_to_klio_message(klio_message, klio_message_str, klio_config, logger):
    actual_message = serializer.to_klio_message(
        klio_message_str, klio_config, logger
    )

    assert klio_message == actual_message
    logger.error.assert_not_called()


def test_to_klio_message_allow_non_kmsg(klio_config, logger, monkeypatch):
    monkeypatch.setattr(
        klio_config.job_config, "allow_non_klio_messages", True
    )
    incoming = b"Not a klio message"
    expected = klio_pb2.KlioMessage()
    expected.data.element = incoming
    expected.version = klio_pb2.Version.V2

    actual_message = serializer.to_klio_message(incoming, klio_config, logger)

    assert expected == actual_message
    logger.error.assert_not_called()


def test_to_klio_message_raises(klio_config, logger, monkeypatch):
    incoming = b"Not a klio message"

    with pytest.raises(gproto_message.DecodeError):
        serializer.to_klio_message(incoming, klio_config, logger)

    # Just asserting it's called - not testing the error string itself
    # to avoid making brittle tests
    assert 1 == logger.error.call_count


@pytest.mark.parametrize(
    "payload,exp_payload",
    (
        (None, None),
        (b"some payload", b"some payload"),
        (_get_klio_message().data, None),
        ("string payload", b"string payload"),
    ),
)
def test_from_klio_message(klio_message, payload, exp_payload):
    expected = _get_klio_message()
    if exp_payload:
        expected.data.payload = exp_payload

    expected_str = expected.SerializeToString()

    actual_message = serializer.from_klio_message(klio_message, payload)
    assert expected_str == actual_message


def test_from_klio_message_v1():
    payload = b"some-payload"
    msg = klio_pb2.KlioMessage()
    msg.version = klio_pb2.Version.V1
    msg.data.payload = payload

    expected_str = msg.SerializeToString()

    actual_message = serializer.from_klio_message(msg, payload)
    assert expected_str == actual_message


def test_from_klio_message_tagged_output(klio_message):
    payload = b"some payload"
    expected_msg = _get_klio_message()
    expected_msg.data.payload = payload

    expected = pvalue.TaggedOutput("a-tag", expected_msg.SerializeToString())

    tagged_payload = pvalue.TaggedOutput("a-tag", payload)
    actual_message = serializer.from_klio_message(klio_message, tagged_payload)

    # can't compare expected vs actual directly since pvalue.TaggedOutput
    # hasn't implemented the comparison operators
    assert expected.tag == actual_message.tag
    assert expected.value == actual_message.value


def test_from_klio_message_raises(klio_message):
    payload = {"no": "bytes casting"}

    with pytest.raises(
        exceptions.KlioMessagePayloadException, match="Returned payload"
    ):
        serializer.from_klio_message(klio_message, payload)

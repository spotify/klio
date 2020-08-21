# Copyright 2019 Spotify AB

import pytest

from google.protobuf import message as gproto_message

from klio_core.proto.v1beta1 import klio_pb2

from klio.message import exceptions
from klio.message import serializer


def _get_klio_job():
    job = klio_pb2.KlioJob()
    job.job_name = "klio-job"
    job.gcp_project = "test-project"
    job_input = job.JobInput()
    job_input.topic = "klio-job-input-topic"
    job_input.subscription = "klio-job-input-subscription"
    job_input.data_location = "klio-job-input-data"
    job.inputs.extend([job_input])
    return job


def _get_klio_message():
    parent_klio_job = _get_klio_job()
    msg = klio_pb2.KlioMessage()
    msg.metadata.visited.extend([parent_klio_job])
    msg.metadata.force = True
    msg.metadata.ping = True
    msg.data.element = b"1234567890"

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


def test_to_klio_message(klio_message, klio_message_str, klio_config, logger):
    actual_message = serializer._to_klio_message(
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

    actual_message = serializer._to_klio_message(incoming, klio_config, logger)

    assert expected == actual_message
    logger.error.assert_not_called()


def test_to_klio_message_raises(klio_config, logger, monkeypatch):
    incoming = b"Not a klio message"

    with pytest.raises(gproto_message.DecodeError):
        serializer._to_klio_message(incoming, klio_config, logger)

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

    actual_message = serializer._from_klio_message(klio_message, payload)
    assert expected == actual_message


def test_from_klio_message_raises(klio_message):
    payload = {"no": "bytes casting"}

    with pytest.raises(
        exceptions.KlioMessagePayloadException, match="Returned payload"
    ):
        serializer._from_klio_message(klio_message, payload)

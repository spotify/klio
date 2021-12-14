# Copyright 2021 Spotify AB
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
import threading

from unittest import mock

import apache_beam as beam
import pytest

from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.testing import test_pipeline as beam_test_pipeline

from klio_core.proto import klio_pb2

from klio.message import pubsub_message_manager as pmm
from klio.transforms import core
from tests.unit import conftest

# NOTE: Since some helper transforms use some decorators that access config, we
# just patch on the module level instead of within each and every test
# function.
patcher = mock.patch.object(core.RunConfig, "get", conftest._klio_config)
patcher.start()


from klio.transforms import helpers  # NOQA: E402, I100, I202

patcher.stop()


@pytest.fixture
def patch_subscriber_client(mocker, monkeypatch):
    # patch out network calls in SubscriberClient instantiation
    c = mocker.Mock()
    monkeypatch.setattr(pmm.g_pubsub, "SubscriberClient", c)
    return c


@pytest.fixture
def msg_manager(patch_subscriber_client, mocker, monkeypatch):
    m = pmm.MessageManager("subscription")
    mock_threadpool_exec = mocker.Mock()
    monkeypatch.setattr(m, "executor", mock_threadpool_exec)
    return m


@pytest.fixture
def pubsub_klio_msg():
    m = pmm.PubSubKlioMessage(ack_id=1, kmsg_id=2)
    return m


def test_pubsub_klio_msg(pubsub_klio_msg):
    assert 1 == pubsub_klio_msg.ack_id
    assert 2 == pubsub_klio_msg.kmsg_id

    assert pubsub_klio_msg.last_extended is None
    assert pubsub_klio_msg.ext_duration is None
    assert isinstance(pubsub_klio_msg.event, threading.Event)
    assert "PubSubKlioMessage(kmsg_id=2)" == repr(pubsub_klio_msg)


def test_pubsub_klio_msg_extend(mocker, monkeypatch, pubsub_klio_msg):
    exp_last_ts = 524235.294449525
    ts_fn_mock = mocker.Mock(return_value=exp_last_ts)
    monkeypatch.setattr(pmm.time, "monotonic", ts_fn_mock)
    exp_dur = 10
    pubsub_klio_msg.extend(exp_dur)

    assert exp_dur == pubsub_klio_msg.ext_duration
    assert exp_last_ts == pubsub_klio_msg.last_extended


@pytest.fixture
def mm_logger():
    return pmm.logging.getLogger("klio.gke_direct_runner.message_manager")


@pytest.fixture
def hb_logger():
    return pmm.logging.getLogger("klio.gke_direct_runner.heartbeat")


@pytest.fixture
def mock_thread(mocker):
    t = mocker.Mock()
    return t


@pytest.fixture
def patch_thread_init(mocker, monkeypatch, mock_thread):
    m = mocker.Mock(return_value=mock_thread)
    monkeypatch.setattr(pmm.threading, "Thread", m)
    return m


@pytest.mark.parametrize(
    "sleep_args, exp_hb_sleep, exp_mgr_sleep",
    [
        ({}, 10, 3),
        ({"heartbeat_sleep": 15}, 15, 3),
        ({"manager_sleep": 15}, 10, 15),
        ({"heartbeat_sleep": 15, "manager_sleep": 15}, 15, 15),
    ],
)
def test_msg_manager_init(
    patch_subscriber_client,
    mm_logger,
    hb_logger,
    sleep_args,
    exp_hb_sleep,
    exp_mgr_sleep,
):
    exp_subname = "some/subscription/name"
    mm = pmm.MessageManager(sub_name=exp_subname, **sleep_args)
    patch_subscriber_client.assert_called_once_with()
    assert exp_subname == mm._sub_name
    assert exp_hb_sleep == mm.heartbeat_sleep
    assert exp_mgr_sleep == mm.manager_sleep
    assert 0 == len(mm.messages)
    assert mm_logger == mm.mgr_logger
    assert hb_logger == mm.hrt_logger


# This plus the MockTrueFunc class is meant to mock the `foo in my_dict`
# call, particularly helpful when needing to break out of a `while True` loop.
# I couldn't otherwise find a way to mock a dictionary to return a different
# value for the same key lookup.
def true_once():
    yield True
    yield False


class MockTrueFunc:
    def __init__(self):
        self.gen = true_once()

    def __call__(self, *args, **kwargs):
        return next(self.gen)


def test_msg_manager_manage(mocker, monkeypatch, msg_manager):
    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)

    maybe_extend = mocker.Mock()
    monkeypatch.setattr(msg_manager, "_maybe_extend", maybe_extend)
    mock_rm = mocker.Mock()
    monkeypatch.setattr(msg_manager, "remove", mock_rm)

    mock_entity_id_to_ack_id = mock.MagicMock()
    mock_entity_id_to_ack_id.__contains__ = MockTrueFunc()
    monkeypatch.setattr(pmm, "ENTITY_ID_TO_ACK_ID", mock_entity_id_to_ack_id)

    msg = mocker.Mock(kmsg_id=1)
    msg_manager.manage(msg)

    maybe_extend.assert_called_once_with(msg)
    mock_rm.assert_called_once_with(msg)
    mock_time.sleep.assert_called_once_with(msg_manager.manager_sleep)


def test_msg_manager_heartbeat(mocker, monkeypatch, msg_manager, caplog):
    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)

    maybe_extend = mocker.Mock()
    monkeypatch.setattr(msg_manager, "_maybe_extend", maybe_extend)
    mock_rm = mocker.Mock()
    monkeypatch.setattr(msg_manager, "remove", mock_rm)

    mock_entity_id_to_ack_id = mock.MagicMock()
    mock_entity_id_to_ack_id.__contains__ = MockTrueFunc()
    monkeypatch.setattr(pmm, "ENTITY_ID_TO_ACK_ID", mock_entity_id_to_ack_id)

    msg = mocker.Mock(kmsg_id=1)
    msg_manager.heartbeat(msg)

    assert 2 == len(caplog.records)
    mock_time.sleep.assert_called_once_with(msg_manager.heartbeat_sleep)


@pytest.mark.parametrize(
    "last_extended, deadline_extended",
    [
        # > threshold of 8s before the "now" timestamp of 15
        (6.9, True),
        # == threshold of 8s before the "now" timestamp of 15
        (7, True),
        # < threshold of 8s before the "now" timestamp of 15
        (10, False),
    ],
)
def test_msg_manager_maybe_extend(
    mocker, monkeypatch, msg_manager, last_extended, deadline_extended, caplog,
):
    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)
    mock_time.monotonic.return_value = 15

    mock_ext_deadline = mocker.Mock()
    monkeypatch.setattr(msg_manager, "extend_deadline", mock_ext_deadline)

    kmsg = pmm.PubSubKlioMessage(ack_id=1, kmsg_id=2)
    kmsg.last_extended = last_extended
    kmsg.ext_duration = 10  # threshold = 0.8 * 10 = 8s

    msg_manager._maybe_extend(kmsg)

    mgr_logs = [
        c
        for c in caplog.records
        if c.name == msg_manager.mgr_logger.name and c.levelno == logging.DEBUG
    ]

    if deadline_extended:
        mock_ext_deadline.assert_called_once_with(kmsg)
        assert 0 == len(mgr_logs)
    else:
        mock_ext_deadline.assert_not_called()
        assert 1 == len(mgr_logs)


@pytest.mark.parametrize("duration", (None, 1))
def test_msg_manager_extend_deadline(mocker, msg_manager, duration, caplog):
    kmsg = pmm.PubSubKlioMessage(ack_id=1, kmsg_id=2)
    kmsg.extend = mocker.Mock()
    msg_manager.extend_deadline(kmsg, duration)

    exp_duration = (
        duration if duration else msg_manager.DEFAULT_DEADLINE_EXTENSION
    )

    exp_req = {
        "subscription": msg_manager._sub_name,
        "ack_ids": [kmsg.ack_id],
        "ack_deadline_seconds": exp_duration,
    }
    msg_manager._client.modify_ack_deadline.assert_called_once_with(**exp_req)

    assert 1 == len(caplog.records)
    kmsg.extend.assert_called_once_with(exp_duration)


def test_msg_manager_extend_deadline_raises(msg_manager, caplog):
    kmsg = pmm.PubSubKlioMessage(ack_id=1, kmsg_id=2)
    msg_manager._client.modify_ack_deadline.side_effect = Exception("oh no")
    msg_manager.extend_deadline(kmsg, 12)
    assert 2 == len(caplog.records)


def _get_pubsub_message(klio_element):
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(klio_element.encode("utf-8"))
    kmsg_bytes = kmsg.SerializeToString()
    pmsg = beam_pubsub.PubsubMessage(data=kmsg_bytes, attributes={})
    return pmsg


def _compare_objects_dicts(first, second):
    return first.__dict__ == second.__dict__


def test_convert_raw_pubsub_message(mocker, monkeypatch, msg_manager):
    mock_event = mocker.Mock()
    monkeypatch.setattr(pmm.threading, "Event", mock_event)
    exp_message = pmm.PubSubKlioMessage("ack_id1", "kmsg_id1")

    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = b"kmsg_id1"
    kmsg_bytes = kmsg.SerializeToString()
    pmsg = beam_pubsub.PubsubMessage(data=kmsg_bytes, attributes={})

    act_message = msg_manager._convert_raw_pubsub_message("ack_id1", pmsg)
    # comparing class attributes (via __dict__) since we'd need to implement
    # __eq__ on the PubSubKlioMessage class, but doing so would make it un-
    # hashable. Which can be addressed, but this just seems easier for now.
    assert _compare_objects_dicts(exp_message, act_message)


def test_msg_manager_add(mocker, monkeypatch, msg_manager, caplog):
    extend_deadline = mocker.Mock()
    monkeypatch.setattr(msg_manager, "extend_deadline", extend_deadline)
    mock_event = mocker.Mock()
    monkeypatch.setattr(pmm.threading, "Event", mock_event)

    pmsg1 = _get_pubsub_message("2")
    pmsg2 = _get_pubsub_message("4")
    psk_msg1 = pmm.PubSubKlioMessage(ack_id=1, kmsg_id="2")
    psk_msg2 = pmm.PubSubKlioMessage(ack_id=3, kmsg_id="4")

    msg_manager.add(ack_id=1, raw_pubsub_message=pmsg1)
    msg_manager.add(ack_id=3, raw_pubsub_message=pmsg2)

    assert 4 == msg_manager.executor.submit.call_count
    assert 2 == len(caplog.records)
    assert _compare_objects_dicts(
        psk_msg1, pmm.ENTITY_ID_TO_ACK_ID[psk_msg1.kmsg_id]
    )
    assert _compare_objects_dicts(
        psk_msg2, pmm.ENTITY_ID_TO_ACK_ID[psk_msg2.kmsg_id]
    )


def test_msg_manager_remove(mocker, monkeypatch, msg_manager, caplog):
    ack_id, kmsg_id = 1, "2"
    psk_msg1 = pmm.PubSubKlioMessage(ack_id, kmsg_id)

    msg_manager.remove(psk_msg1)
    msg_manager._client.acknowledge.assert_called_once_with(
        msg_manager._sub_name, [ack_id]
    )
    assert 1 == len(caplog.records)


def test_msg_manager_remove_raises(msg_manager, monkeypatch, caplog):
    ack_id, kmsg_id = 1, "2"
    psk_msg1 = pmm.PubSubKlioMessage(ack_id, kmsg_id)

    msg_manager._client.acknowledge.side_effect = Exception("oh no")
    msg_manager.remove(psk_msg1)
    assert 2 == len(caplog.records)


def _generate_kmsg(element):
    message = klio_pb2.KlioMessage()
    message.data.element = bytes(str(element), "utf-8")
    return message.SerializeToString()


def _assert_expected_msg(actual):
    actual_msg = klio_pb2.KlioMessage()
    actual_msg.ParseFromString(actual)

    expected_msg = klio_pb2.KlioMessage()
    expected_msg.data.element = b"2"
    assert actual_msg == expected_msg


def test_klio_ack_input_msg(mocker, monkeypatch):
    ack_id, kmsg_id = 1, "2"
    psk_msg1 = pmm.PubSubKlioMessage(ack_id, kmsg_id)
    monkeypatch.setitem(pmm.ENTITY_ID_TO_ACK_ID, psk_msg1.kmsg_id, psk_msg1)
    with beam_test_pipeline.TestPipeline() as p:
        (
            p
            | beam.Create([kmsg_id])
            | beam.Map(_generate_kmsg)
            | beam.ParDo(helpers.KlioAckInputMessage())
            | beam.Map(_assert_expected_msg)
        )

    assert not pmm.ENTITY_ID_TO_ACK_ID.get("2")

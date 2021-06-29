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

import apache_beam as beam
import pytest

from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.testing import test_pipeline as beam_test_pipeline

from klio_core.proto import klio_pb2

from klio.message import pubsub_message_manager as pmm
from klio.transforms import helpers


@pytest.fixture
def patch_subscriber_client(mocker, monkeypatch):
    # patch out network calls in SubscriberClient instantiation
    c = mocker.Mock()
    monkeypatch.setattr(pmm.g_pubsub, "SubscriberClient", c)
    return c


@pytest.fixture
def msg_manager(patch_subscriber_client):
    m = pmm.MessageManager("subscription")
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
        ({}, 10, 5),
        ({"heartbeat_sleep": 15}, 15, 5),
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
    # threading.Lock is a factory that outputs the right
    # lock implementation for the current platform
    assert isinstance(mm.messages_lock, type(threading.Lock()))
    assert mm_logger == mm.mgr_logger
    assert hb_logger == mm.hrt_logger


def test_msg_manager_start_threads(
    mocker, msg_manager, patch_thread_init, mock_thread
):
    msg_manager.start_threads()
    mgr_call = mocker.call(
        target=msg_manager.manage,
        args=(msg_manager.manager_sleep,),
        name="KlioMessageManager",
        daemon=True,
    )
    hb_call = mocker.call(
        target=msg_manager.heartbeat,
        args=(msg_manager.heartbeat_sleep,),
        name="KlioMessageHeartbeat",
        daemon=True,
    )
    patch_thread_init.assert_has_calls([mgr_call, hb_call])
    assert 2 == len(mock_thread.start.mock_calls)


def test_msg_manager_manage(mocker, monkeypatch, msg_manager):
    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)
    stop_loop_exception = Exception("exit loop")
    mock_time.sleep.side_effect = [None, stop_loop_exception]

    msg_manager.messages = [mocker.Mock(kmsg_id=1), mocker.Mock(kmsg_id=2)]

    ext_or_rm = [True, False, False, False]
    mock_ext_or_rm = mocker.Mock()
    mock_ext_or_rm.side_effect = ext_or_rm
    monkeypatch.setattr(msg_manager, "_extend_or_remove", mock_ext_or_rm)

    mock_rm = mocker.Mock()
    monkeypatch.setattr(msg_manager, "remove", mock_rm)

    to_slp = 0.1
    try:
        msg_manager.manage(to_slp)
    except Exception as e:
        # do this check so that we don't accidentally
        # swallow a real exception
        assert e == stop_loop_exception

    mock_ext_or_rm.assert_has_calls(
        [
            mocker.call(msg_manager.messages[0]),
            mocker.call(msg_manager.messages[1]),
        ]
        * 2
    )
    mock_rm.assert_called_once_with(msg_manager.messages[0])
    mock_time.sleep.assert_has_calls([mocker.call(to_slp)] * 2)


def test_msg_manager_heartbeat(mocker, monkeypatch, msg_manager):
    msg_manager.messages = [mocker.Mock(kmsg_id=1)]

    # set the last log message as an exception so that we
    # can exit the while loop
    mock_logger = mocker.Mock()
    monkeypatch.setattr(msg_manager, "hrt_logger", mock_logger)
    stop_loop_exception = Exception("exit loop")
    mock_logger.info.side_effect = [None, stop_loop_exception]

    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)

    try:
        msg_manager.heartbeat(1)
    except Exception as e:
        # make sure we haven't swallowed a real exception
        assert e == stop_loop_exception

    assert 2 == len(mock_logger.mock_calls)
    assert mock_time.sleep.called_once_with(1)


@pytest.mark.parametrize("processing_complete", (True, False))
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
def test_msg_manager_ext_or_rm(
    mocker,
    monkeypatch,
    msg_manager,
    processing_complete,
    last_extended,
    deadline_extended,
    caplog,
):
    mock_time = mocker.Mock()
    monkeypatch.setattr(pmm, "time", mock_time)
    mock_time.monotonic.return_value = 15

    mock_ext_deadline = mocker.Mock()
    monkeypatch.setattr(msg_manager, "extend_deadline", mock_ext_deadline)

    kmsg = pmm.PubSubKlioMessage(ack_id=1, kmsg_id=2)
    kmsg.last_extended = last_extended
    kmsg.ext_duration = 10  # threshold = 0.8 * 10 = 8s
    kmsg.event.is_set = mocker.Mock(return_value=processing_complete)
    actual = msg_manager._extend_or_remove(kmsg)

    assert processing_complete == actual
    mgr_logs = [
        c
        for c in caplog.records
        if c.name == msg_manager.mgr_logger.name and c.levelno == logging.DEBUG
    ]
    if not processing_complete:
        if deadline_extended:
            mock_ext_deadline.assert_called_once_with(kmsg)
            assert 0 == len(mgr_logs)
        else:
            mock_ext_deadline.assert_not_called()
            assert 1 == len(mgr_logs)
    else:
        mock_ext_deadline.assert_not_called()


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

    assert 2 == len(msg_manager.messages)
    # comparing class attributes (via __dict__) since we'd need to implement
    # __eq__ on the PubSubKlioMessage class, but doing so would make it un-
    # hashable. Which can be addressed, but this just seems easier for now.
    assert _compare_objects_dicts(psk_msg1, msg_manager.messages[0])
    assert _compare_objects_dicts(psk_msg2, msg_manager.messages[1])
    assert 2 == len(caplog.records)
    assert _compare_objects_dicts(
        psk_msg1, pmm.ENTITY_ID_TO_ACK_ID[psk_msg1.kmsg_id]
    )
    assert _compare_objects_dicts(
        psk_msg2, pmm.ENTITY_ID_TO_ACK_ID[psk_msg2.kmsg_id]
    )


def test_msg_manager_remove(mocker, monkeypatch, msg_manager, caplog):
    # mock_event = mocker.Mock()
    # monkeypatch.setattr(pmm.threading, "Event", mock_event)
    # extend_deadline = mocker.Mock()
    # monkeypatch.setattr(msg_manager, "extend_deadline", extend_deadline)

    ack_id1, kmsg_id1 = 1, "2"
    ack_id2, kmsg_id2 = 3, "4"
    pmsg1 = _get_pubsub_message(kmsg_id1)
    pmsg2 = _get_pubsub_message(kmsg_id2)

    msg_manager.add(ack_id=ack_id2, raw_pubsub_message=pmsg2)
    msg_manager.add(ack_id=ack_id1, raw_pubsub_message=pmsg1)

    msg_manager.remove(msg_manager.messages[0])
    msg_manager._client.acknowledge.assert_called_once_with(
        msg_manager._sub_name, [ack_id2]
    )
    assert not pmm.ENTITY_ID_TO_ACK_ID.get(kmsg_id2)
    assert 5 == len(caplog.records)
    assert 1 == len(msg_manager.messages) == len(pmm.ENTITY_ID_TO_ACK_ID)


def test_msg_manager_remove_raises(msg_manager, caplog):
    pmsg1 = _get_pubsub_message("2")

    msg_manager.add(ack_id=1, raw_pubsub_message=pmsg1)
    msg_manager._client.acknowledge.side_effect = Exception("oh no")
    msg_manager.remove(msg_manager.messages[0])
    assert 4 == len(caplog.records)
    assert not pmm.ENTITY_ID_TO_ACK_ID.get("2")
    assert 0 == len(msg_manager.messages)


def _generate_kmsg(element):
    message = klio_pb2.KlioMessage()
    message.data.element = bytes(str(element), "utf-8")
    return message.SerializeToString()


def _assert_expected_msg(actual):
    actual_msg = klio_pb2.KlioMessage()
    actual_msg.ParseFromString(actual)

    expected_msg = klio_pb2.KlioMessage()
    expected_msg.data.element = b"d34db33f"
    assert actual_msg == expected_msg


def test_klio_ack_input_msg(mocker):
    entity_id = "d34db33f"
    mock_pklio_msg = mocker.Mock()
    pmm.ENTITY_ID_TO_ACK_ID[entity_id] = mock_pklio_msg
    with beam_test_pipeline.TestPipeline() as p:
        (
            p
            | beam.Create([entity_id])
            | beam.Map(_generate_kmsg)
            | beam.ParDo(helpers.KlioAckInputMessage())
            | beam.Map(_assert_expected_msg)
        )

    mock_pklio_msg.event.set.assert_called_once_with()

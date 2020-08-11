# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import logging
import types

import pytest

from google.api_core import exceptions as gapi_exceptions
from google.protobuf import message as gproto_message

from klio_core import config
from klio_core import utils
from klio_core.proto.v1beta1 import klio_pb2

from klio.message_handler import v1 as message_handler
from klio.transforms import core


@pytest.fixture
def klio_job_config():
    conf = {
        "number_of_retries": 0,
        "inputs": [
            {
                "topic": "projects/sigint/topics/test-klio-parent-message-out",
                "subscription": (
                    "projects/sigint/subscription/test-klio-parent-message-"
                    "out-test-job"
                ),
                "data_location": "gs://sigint-output/test-klio-parent-out",
            }
        ],
        "outputs": [
            {
                "topic": "projects/sigint/topics/test-klio-message-out",
                "data_location": "gs://sigint-output/test-klio-out",
            }
        ],
        "dependencies": [
            {"gcp_project": "sigint", "job_name": "test-klio-parent-job"}
        ],
    }
    return config.KlioJobConfig(
        job_name="test-klio-job", config_dict=conf, version=1
    )


@pytest.fixture
def klio_job():
    job = klio_pb2.KlioJob()
    job.job_name = "klio-job"
    job.gcp_project = "test-project"
    job_input = job.JobInput()
    job_input.topic = "klio-job-input-topic"
    job_input.subscription = "klio-job-input-subscription"
    job_input.data_location = "klio-job-input-data"
    job.inputs.extend([job_input])
    return job


@pytest.fixture
def parent_klio_job():
    parent_job = klio_pb2.KlioJob()
    parent_job.job_name = "parent-klio-job"
    parent_job.gcp_project = "another-test-project"
    job_input = parent_job.JobInput()
    job_input.topic = "parent-klio-job-input-topic"
    job_input.data_location = "parent-klio-job-input-data"
    parent_job.inputs.extend([job_input])
    return parent_job


@pytest.fixture
def klio_job_audit_log_item(klio_job):
    audit_item = klio_pb2.KlioJobAuditLogItem()
    audit_item.timestamp.FromJsonString("2019-01-01T00:00:00Z")
    audit_item.klio_job.CopyFrom(klio_job)
    return audit_item


@pytest.fixture
def klio_message(klio_job, parent_klio_job):
    msg = klio_pb2.KlioMessage()
    msg.metadata.visited.extend([parent_klio_job])
    msg.metadata.force = True
    msg.metadata.ping = True
    msg.data.v1.entity_id = "1234567890"

    return msg


@pytest.fixture
def klio_message_str(klio_message):
    return klio_message.SerializeToString()


@pytest.fixture
def mock_get_publisher(mocker, monkeypatch):
    mock_get_publisher = mocker.Mock()
    monkeypatch.setattr(utils, "get_publisher", mock_get_publisher)
    return mock_get_publisher


@pytest.fixture
def mock_timeout_process(mocker):
    return mocker.patch.object(
        message_handler.timeout_process, "TimeoutProcess"
    )


@pytest.fixture
def mock_timeout_dofn_process(mocker):
    return mocker.patch.object(message_handler, "timeout_dofn_process")


def test_timeout_process(mocker, user_dofn, caplog):
    entity_id = 123
    timeout_threshold = 30
    n_retries = 2
    mock_process = mocker.Mock()
    dofn_inst = user_dofn()
    mock_retry_process = mocker.patch.object(message_handler, "retry_process")

    mock_thread_pool = mocker.patch.object(core._KlioNamespace, "_thread_pool")

    mock_mapped_result = mock_thread_pool.starmap_async.return_value
    mock_mapped_result.ready.return_value = True

    def process(*args, **kwargs):
        mock_process.return_value = "not a generator"
        mock_process(*args, **kwargs)
        return mock_process.return_value

    actual = message_handler.timeout_wrapped_process(
        dofn_inst, entity_id, process, timeout_threshold, n_retries
    )
    mock_thread_pool.starmap_async.assert_called_once_with(
        mock_retry_process,
        iterable=[
            (dofn_inst, entity_id, process, timeout_threshold, n_retries)
        ],
    )
    assert mock_mapped_result == actual


def test_timeout_dofn_process(mocker, mock_timeout_process, user_dofn, caplog):
    timeout_threshold = 5
    entity_id = "1234567890"
    dofn_inst = user_dofn()
    klio_logger = dofn_inst._klio.logger

    called_process = mock_timeout_process.return_value
    called_process.exception = None
    called_process.is_alive.return_value = False

    message_handler.timeout_dofn_process(
        called_process, timeout_threshold, klio_logger, entity_id
    )

    called_process.start.assert_called_once_with()
    called_process.join.assert_called_once_with(timeout_threshold)
    called_process.terminate.assert_not_called()
    assert 2 == len(caplog.records)
    for r in range(len(caplog.records)):
        assert "DEBUG" == caplog.records[r].levelname


def test_timeout_dofn_process_exception(
    mock_timeout_process, user_dofn, caplog
):
    timeout_threshold = 5
    entity_id = "1234567890"
    dofn_inst = user_dofn()
    klio_logger = dofn_inst._klio.logger

    called_process = mock_timeout_process.return_value
    called_process.exception = "Some Exception"

    with pytest.raises(Exception):
        message_handler.timeout_dofn_process(
            called_process, timeout_threshold, klio_logger, entity_id
        )

    called_process.start.assert_called_once_with()
    called_process.join.assert_called_once_with(timeout_threshold)
    assert 2 == len(caplog.records)
    for r in range(len(caplog.records) - 1):
        assert "DEBUG" == caplog.records[r].levelname


def test_timeout_dofn_process_raises_timeout_error(
    mocker, mock_timeout_process, user_dofn, caplog
):
    timeout_threshold = 5
    entity_id = "1234567890"
    dofn_inst = user_dofn()
    klio_logger = dofn_inst._klio.logger

    called_process = mock_timeout_process.return_value
    called_process.exception = None
    called_process.is_alive.return_value = True

    with pytest.raises(Exception):
        message_handler.timeout_dofn_process(
            called_process, timeout_threshold, klio_logger, entity_id
        )

    called_process.start.assert_called_once_with()
    called_process.join.assert_called_once_with(timeout_threshold)
    called_process.terminate.assert_called_once_with()
    assert 2 == len(caplog.records)
    for r in range(len(caplog.records)):
        assert "DEBUG" == caplog.records[r].levelname


@pytest.mark.parametrize("is_generator", (True, False))
def test_retry_process(user_dofn, caplog, mocker, is_generator):
    n_retries = 2
    entity_id = "1234567890"
    dofn_inst = user_dofn()

    mock_process = mocker.Mock()

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    r_value = message_handler.retry_process(
        dofn_inst, entity_id, process, n_retries
    )

    assert 1 == mock_process.call_count
    assert r_value is None


def test_retry_process_exhaust_retries(user_dofn, mocker, monkeypatch):
    n_retries = 2
    entity_id = "1234567890"
    dofn_inst = user_dofn()

    mock_process = mocker.Mock()
    mock_process.side_effect = [
        Exception("foo"),
        Exception("bar"),
        Exception("baz"),
    ]

    with pytest.raises(Exception):
        message_handler.retry_process(
            dofn_inst, entity_id, mock_process, n_retries
        )

    assert 3 == mock_process.call_count


def test_retry_process_no_retries(user_dofn, mocker, monkeypatch):
    n_retries = -1
    entity_id = "1234567890"
    dofn_inst = user_dofn()

    mock_process = mocker.Mock()

    r_value = message_handler.retry_process(
        dofn_inst, entity_id, mock_process, n_retries
    )

    mock_process.assert_not_called()
    assert r_value is None


@pytest.mark.parametrize("is_generator", (True, False))
def test_retry_process_with_timeout(
    user_dofn, mocker, is_generator, mock_timeout_dofn_process
):
    n_retries = 2
    timeout_threshold = 30
    entity_id = "1234567890"
    dofn_inst = user_dofn()

    mock_process = mocker.Mock()

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    message_handler.retry_process(
        dofn_inst, entity_id, process, n_retries, timeout_threshold
    )

    assert 1 == mock_timeout_dofn_process.call_count


@pytest.mark.parametrize("is_generator", (True, False))
def test_retry_process_with_timeout_raises(
    user_dofn, caplog, mocker, is_generator, mock_timeout_dofn_process
):
    n_retries = 2
    timeout_threshold = 30
    entity_id = "1234567890"
    dofn_inst = user_dofn()
    mock_timeout_dofn_process.side_effect = Exception

    mock_process = mocker.Mock()

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    with pytest.raises(Exception):
        message_handler.retry_process(
            dofn_inst, entity_id, process, n_retries, timeout_threshold
        )

    assert 3 == mock_timeout_dofn_process.call_count
    assert 3 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname


@pytest.mark.parametrize("is_generator", (True, False))
def test_retry_process_raises_exception(
    user_dofn, caplog, mocker, is_generator
):
    n_retries = 2
    entity_id = "1234567890"
    dofn_inst = user_dofn()

    mock_process = mocker.Mock()
    mock_process.side_effect = Exception

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    with pytest.raises(Exception):
        message_handler.retry_process(dofn_inst, entity_id, process, n_retries)

    assert 3 == mock_process.call_count
    assert 3 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname


@pytest.mark.parametrize("parent_in_downstream", (True, False))
def test_trigger_parent_jobs(
    parent_in_downstream,
    user_dofn,
    klio_message,
    parent_klio_job,
    mock_get_publisher,
    caplog,
    mocker,
):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "parent_jobs")
    dofn_inst._klio.parent_jobs = [parent_klio_job.SerializeToString()]

    if parent_in_downstream:
        klio_message.metadata.downstream.extend([parent_klio_job])

    message_handler.trigger_parent_jobs(dofn_inst, klio_message)

    mock_get_publisher.return_value.publish.assert_called_once_with(
        "parent-klio-job-input-topic", klio_message.SerializeToString()
    )
    assert 1 == len(caplog.records)


def test_trigger_parent_jobs_no_parents_warns(user_dofn, klio_message, mocker):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "parent_jobs")
    dofn_inst._klio.parent_jobs = []
    with pytest.warns(RuntimeWarning):
        message_handler.trigger_parent_jobs(dofn_inst, klio_message)


def test_trigger_parent_jobs_get_publiser_raises(
    user_dofn,
    parent_klio_job,
    klio_message,
    mock_get_publisher,
    caplog,
    mocker,
):
    mock_get_publisher.side_effect = gapi_exceptions.GoogleAPIError("foo")

    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "parent_jobs")
    dofn_inst._klio.parent_jobs = [parent_klio_job.SerializeToString()]

    ret = message_handler.trigger_parent_jobs(dofn_inst, klio_message)

    assert ret is None
    assert 1 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname


def test_trigger_parent_jobs_publish_raises(
    user_dofn,
    parent_klio_job,
    klio_message,
    mock_get_publisher,
    caplog,
    mocker,
):
    mock_pub_client = mock_get_publisher.return_value
    mock_pub_client.publish.side_effect = gapi_exceptions.GoogleAPIError("foo")

    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "parent_jobs")
    dofn_inst._klio.parent_jobs = [parent_klio_job.SerializeToString()]

    ret = message_handler.trigger_parent_jobs(dofn_inst, klio_message)

    assert ret is None
    mock_pub_client.publish.assert_called_once_with(
        "parent-klio-job-input-topic", klio_message.SerializeToString()
    )
    assert 1 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname


@pytest.mark.parametrize(
    "data_exists,current_in_downstream",
    ((True, False), (False, True), (False, False)),
)
def test_check_input_data_exists(
    data_exists,
    current_in_downstream,
    user_dofn,
    klio_message,
    klio_job,
    mocker,
    monkeypatch,
    caplog,
):
    dofn_inst = user_dofn()
    mock_input_data_exists = mocker.Mock(return_value=data_exists)
    monkeypatch.setattr(dofn_inst, "input_data_exists", mock_input_data_exists)

    mock_trigger_parent_jobs = mocker.Mock()
    monkeypatch.setattr(
        message_handler, "trigger_parent_jobs", mock_trigger_parent_jobs
    )

    if current_in_downstream:
        klio_message.metadata.downstream.extend([klio_job])

    ret_msg_state = message_handler.check_input_data_exists(
        dofn_inst, klio_message, klio_job
    )

    mock_input_data_exists.assert_called_once_with(
        klio_message.data.v1.entity_id
    )
    if not data_exists:
        assert 1 == len(caplog.records)
        mock_trigger_parent_jobs.assert_called_once_with(
            dofn_inst, klio_message
        )
        assert message_handler.MESSAGE_STATE.DROP == ret_msg_state
    else:
        assert not caplog.records
        assert message_handler.MESSAGE_STATE.PROCESS == ret_msg_state


@pytest.mark.parametrize(
    "data_exists,force,expected",
    ((True, True, False), (True, False, True), (False, False, False)),
)
def test_check_output_data_exists(
    data_exists,
    force,
    expected,
    user_dofn,
    klio_message,
    mocker,
    monkeypatch,
    caplog,
):
    dofn_inst = user_dofn()
    mock_output_data_exists = mocker.Mock(return_value=data_exists)
    monkeypatch.setattr(
        dofn_inst, "output_data_exists", mock_output_data_exists
    )
    klio_message.metadata.force = force

    should_skip = message_handler.check_output_data_exists(
        dofn_inst, klio_message
    )

    assert expected == should_skip

    if force or not data_exists:
        assert not caplog.records
    else:
        assert 1 == len(caplog.records)

    if not force:
        mock_output_data_exists.assert_called_once_with(
            klio_message.data.v1.entity_id
        )


def test_update_audit_log(user_dofn, klio_message, klio_job, caplog, mocker):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "job")
    dofn_inst._klio.job = klio_job.SerializeToString()

    ret_message = message_handler.update_audit_log(
        dofn_inst, klio_message, klio_job
    )

    assert 1 == len(caplog.records)
    assert klio_message == ret_message


@pytest.mark.parametrize(
    "downstream,ping_mode,should_skip,input_check,exp_state",
    (
        (None, False, False, True, message_handler.MESSAGE_STATE.PROCESS),
        (True, False, False, True, message_handler.MESSAGE_STATE.PROCESS),
        (None, False, True, False, message_handler.MESSAGE_STATE.PASS_THRU),
        (False, False, False, False, message_handler.MESSAGE_STATE.DROP),
        (None, True, False, False, message_handler.MESSAGE_STATE.PASS_THRU),
    ),
)
def test_preprocess_klio_message(
    downstream,
    ping_mode,
    should_skip,
    input_check,
    exp_state,
    user_dofn,
    klio_job,
    klio_message_str,
    klio_message,
    mocker,
    monkeypatch,
    klio_job_audit_log_item,
    parent_klio_job,
    caplog,
):
    klio_message.metadata.ping = ping_mode

    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "job")
    dofn_inst._klio.job = klio_job.SerializeToString()

    # if downstream is None, don't set it at all
    if downstream is False:
        another_job = klio_pb2.KlioJob()
        another_job.CopyFrom(klio_job)
        another_job.job_name = "not-this-job"
        klio_message.metadata.downstream.extend([another_job])

    if downstream is True:
        klio_message.metadata.downstream.extend([klio_job])

    new_message = klio_pb2.KlioMessage()
    new_message.CopyFrom(klio_message)
    audit_item = klio_pb2.KlioJobAuditLogItem()
    audit_item.timestamp.FromJsonString("2019-01-01T00:00:00Z")
    audit_item.klio_job.CopyFrom(klio_job)
    new_message.metadata.job_audit_log.extend([audit_item])

    mock_update_audit_log = mocker.Mock(return_value=new_message)
    monkeypatch.setattr(
        message_handler, "update_audit_log", mock_update_audit_log
    )
    mock_check_output_data_exists = mocker.Mock(return_value=should_skip)
    monkeypatch.setattr(
        message_handler,
        "check_output_data_exists",
        mock_check_output_data_exists,
    )
    mock_check_input_data_exists = mocker.Mock(
        return_value=message_handler.MESSAGE_STATE.PROCESS
    )
    if not input_check:
        state = message_handler.MESSAGE_STATE.DROP
        mock_check_input_data_exists.return_value = state
    monkeypatch.setattr(
        message_handler,
        "check_input_data_exists",
        mock_check_input_data_exists,
    )

    klio_message_str = klio_message.SerializeToString()

    ret_message, ret_msg_state = message_handler.preprocess_klio_message(
        dofn_inst, klio_message_str
    )

    assert exp_state == ret_msg_state

    expected_message = klio_pb2.KlioMessage()
    expected_message.CopyFrom(klio_message)

    if downstream is not False:
        expected_message.metadata.job_audit_log.extend([audit_item])

    assert expected_message == ret_message

    if input_check:
        mock_update_audit_log.assert_called_once_with(
            dofn_inst, klio_message, klio_job
        )
        mock_check_output_data_exists.assert_called_once_with(
            dofn_inst, new_message
        )

    if downstream is False:
        assert 1 == len(caplog.records)
    else:
        assert not caplog.records

    if should_skip:
        mock_check_input_data_exists.assert_not_called()
        assert expected_message == ret_message
    elif input_check:
        mock_check_input_data_exists.assert_called_once_with(
            dofn_inst, new_message, klio_job
        )


@pytest.mark.parametrize(
    "allow_nonklio, binary, message_type",
    (
        (True, False, "STRING"),
        (True, True, "STRING"),
        (False, False, "KLIO_MESSAGE"),
        (True, False, "KLIO_MESSAGE"),
        (True, True, "KLIO_MESSAGE"),
        (True, True, "BYTES"),
    ),
)
def test_preprocess_klio_message_non_klio(
    user_dofn,
    klio_job,
    mocker,
    monkeypatch,
    allow_nonklio,
    binary,
    message_type,
    klio_message,
):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "job")
    dofn_inst._klio.job = klio_job.SerializeToString()

    config = mocker.Mock()
    config.version = 1
    config.job_config = mocker.Mock()
    config.job_config.allow_non_klio_messages = allow_nonklio
    config.job_config.binary_non_klio_messages = binary

    if message_type == "KLIO_MESSAGE":
        message = klio_message.SerializeToString()
    elif message_type == "STRING":
        message = bytes("I am a String", "UTF-8")
    elif message_type == "BYTES":
        message = bytes([0xC0, 0xAF])  # These bytes are invalid UTF-8

    mock_config = mocker.PropertyMock(return_value=config)
    monkeypatch.setattr(core._KlioNamespace, "config", mock_config)

    ret_message, msg_state = message_handler.preprocess_klio_message(
        dofn_inst, message
    )
    if message_type == "KLIO_MESSAGE":
        assert klio_message.data.v1.entity_id == ret_message.data.v1.entity_id
        assert b"" == ret_message.data.v1.payload
    elif binary:
        assert message == ret_message.data.v1.payload
        assert "" == ret_message.data.v1.entity_id
    else:
        assert "I am a String" == ret_message.data.v1.entity_id
        assert b"" == ret_message.data.v1.payload


@pytest.mark.parametrize(
    "allow_nonklio, binary, message_type, exception_type",
    (
        (False, False, "STRING", gproto_message.DecodeError),
        (False, False, "BYTES", gproto_message.DecodeError),
        (True, False, "BYTES", ValueError),
    ),
)
def test_preprocess_klio_message_non_klio_raises(
    user_dofn,
    klio_job,
    mocker,
    monkeypatch,
    allow_nonklio,
    binary,
    message_type,
    klio_message,
    exception_type,
):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "job")
    dofn_inst._klio.job = klio_job.SerializeToString()

    config = mocker.Mock()
    config.version = 1
    config.job_config = mocker.Mock()
    config.job_config.allow_non_klio_messages = allow_nonklio
    config.job_config.binary_non_klio_messages = binary

    if message_type == "KLIO_MESSAGE":
        message = klio_message.SerializeToString()
    elif message_type == "STRING":
        message = bytes("I am a String", "UTF-8")
    elif message_type == "BYTES":
        message = bytes([0xC0, 0xAF])

    mock_config = mocker.PropertyMock(return_value=config)
    monkeypatch.setattr(core._KlioNamespace, "config", mock_config)

    with pytest.raises(exception_type):
        message_handler.preprocess_klio_message(dofn_inst, message)


@pytest.mark.parametrize("ping_mode,exp_log_lines", ((True, 1), (False, 0)))
def test_postprocess_klio_message(
    klio_message, user_dofn, klio_job, ping_mode, exp_log_lines, caplog, mocker
):
    klio_message.metadata.ping = ping_mode

    expected = klio_pb2.KlioMessage()
    expected.CopyFrom(klio_message)
    expected.metadata.visited.extend([klio_job])

    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "job")
    dofn_inst._klio.job = klio_job.SerializeToString()

    ret_message = message_handler.postprocess_klio_message(
        dofn_inst, klio_message
    )

    assert expected == ret_message
    assert exp_log_lines == len(caplog.records)


@pytest.mark.parametrize(
    "timeout_threshold,is_generator,n_retries,msg_state",
    (
        (30, True, 0, message_handler.MESSAGE_STATE.PROCESS),
        (30, False, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, False, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, True, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, False, 2, message_handler.MESSAGE_STATE.PROCESS),
        (0, True, 2, message_handler.MESSAGE_STATE.PROCESS),
        (30, True, 0, message_handler.MESSAGE_STATE.PASS_THRU),
        (0, False, 1, message_handler.MESSAGE_STATE.PASS_THRU),
    ),
)
def test_parse_klio_message(
    mocker,
    is_generator,
    msg_state,
    user_dofn,
    klio_job_config,
    timeout_threshold,
    n_retries,
):
    mock_process = mocker.Mock()
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")

    mock_timeout_process = mocker.patch.object(
        message_handler, "timeout_wrapped_process"
    )
    mock_timeout_process.return_value.successful.return_value = True

    dofn_inst._klio.config.job_config.timeout_threshold = timeout_threshold
    dofn_inst._klio.config.job_config.number_of_retries = n_retries

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    mock_parsed_msg = mocker.Mock()
    mock_preprocess_klio_message = mocker.patch.object(
        message_handler, "preprocess_klio_message"
    )
    mock_preprocess_klio_message.return_value = (mock_parsed_msg, msg_state)
    mock_postprocess_klio_message = mocker.patch.object(
        message_handler, "postprocess_klio_message"
    )

    mock_retry_process = mocker.patch.object(message_handler, "retry_process")
    wrapped_process = message_handler.parse_klio_message(process)

    # calling `next` since message_handler.parse_klio_message is a generator
    mock_entity_id = mock_parsed_msg.data.v1.entity_id
    next(wrapped_process(dofn_inst, mock_entity_id))

    if msg_state is message_handler.MESSAGE_STATE.PROCESS:

        if timeout_threshold:
            mock_timeout_process.assert_called_once_with(
                dofn_inst,
                mock_entity_id,
                process,
                n_retries,
                timeout_threshold,
            )
        elif n_retries:
            mock_retry_process.assert_called_once_with(
                dofn_inst, mock_entity_id, process, n_retries
            )
        else:
            mock_retry_process.assert_not_called()
            mock_process.assert_called_once_with(dofn_inst, mock_entity_id)
    else:
        mock_retry_process.assert_not_called()
        mock_process.assert_not_called()

    mock_preprocess_klio_message.assert_called_once_with(
        dofn_inst, mock_entity_id
    )
    mock_postprocess_klio_message.assert_called_once_with(
        dofn_inst, mock_parsed_msg
    )


@pytest.mark.parametrize(
    "timeout_threshold,is_generator,n_retries,msg_state",
    (
        (30, True, 0, message_handler.MESSAGE_STATE.PROCESS),
        (30, False, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, False, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, True, 0, message_handler.MESSAGE_STATE.PROCESS),
        (0, False, 2, message_handler.MESSAGE_STATE.PROCESS),
        (0, True, 2, message_handler.MESSAGE_STATE.PROCESS),
        (30, True, 0, message_handler.MESSAGE_STATE.PASS_THRU),
        (0, False, 1, message_handler.MESSAGE_STATE.PASS_THRU),
    ),
)
def test_parse_klio_message_raises(
    mocker,
    is_generator,
    msg_state,
    user_dofn,
    klio_job_config,
    timeout_threshold,
    n_retries,
):
    mock_process = mocker.Mock()
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")

    dofn_inst._klio.config.job_config.timeout_threshold = timeout_threshold
    dofn_inst._klio.config.job_config.number_of_retries = n_retries

    if is_generator:

        def process(*args, **kwargs):
            mock_process.return_value = iter("a generator")
            mock_process(*args, **kwargs)
            yield mock_process.return_value

    else:

        def process(*args, **kwargs):
            mock_process.return_value = "not a generator"
            mock_process(*args, **kwargs)
            return mock_process.return_value

    mock_process.side_effect = Exception

    mock_parsed_msg = mocker.Mock()
    mock_preprocess_klio_message = mocker.patch.object(
        message_handler, "preprocess_klio_message"
    )
    mock_preprocess_klio_message.return_value = (mock_parsed_msg, msg_state)
    mock_postprocess_klio_message = mocker.patch.object(
        message_handler, "postprocess_klio_message"
    )

    mock_timeout_process = mocker.patch.object(
        message_handler, "timeout_wrapped_process"
    )
    mock_timeout_process.return_value.successful.return_value = False

    mock_retry_process = mocker.patch.object(message_handler, "retry_process")
    mock_retry_process.side_effect = Exception

    wrapped_process = message_handler.parse_klio_message(process)

    # calling `next` since message_handler.parse_klio_message is a generator
    mock_entity_id = mock_parsed_msg.data.v1.entity_id
    try:
        next(wrapped_process(dofn_inst, mock_entity_id))
    except StopIteration:
        pass

    if msg_state is message_handler.MESSAGE_STATE.PROCESS:

        if timeout_threshold:
            mock_timeout_process.assert_called_once_with(
                dofn_inst,
                mock_entity_id,
                process,
                n_retries,
                timeout_threshold,
            )
        elif n_retries:
            mock_retry_process.assert_called_once_with(
                dofn_inst, mock_entity_id, process, n_retries
            )
        else:
            mock_retry_process.assert_not_called()
            mock_process.assert_called_once_with(dofn_inst, mock_entity_id)
    else:
        mock_retry_process.assert_not_called()
        mock_process.assert_not_called()

    mock_preprocess_klio_message.assert_called_once_with(
        dofn_inst, mock_entity_id
    )

    if msg_state == message_handler.MESSAGE_STATE.PASS_THRU:
        mock_postprocess_klio_message.assert_called_once_with(
            dofn_inst, mock_parsed_msg
        )
    else:
        mock_postprocess_klio_message.assert_not_called()


@pytest.mark.parametrize(
    "is_dropped,preproc_raises", ((True, True), (True, False), (False, False))
)
def test_parse_klio_message_dropped(
    mocker, user_dofn, is_dropped, preproc_raises, klio_message, caplog
):
    dofn_inst = user_dofn()
    mocker.patch.object(core._KlioNamespace, "_load_config_from_file")
    mocker.patch.object(core._KlioNamespace, "logger")
    dofn_inst._klio.logger = logging.getLogger("klio")

    mock_preprocess_klio_message = mocker.patch.object(
        message_handler, "preprocess_klio_message"
    )
    if preproc_raises:
        mock_preprocess_klio_message.side_effect = Exception("fuu")
    elif is_dropped:
        mock_preprocess_klio_message.return_value = (
            klio_message,
            message_handler.MESSAGE_STATE.DROP,
        )
    else:
        mock_preprocess_klio_message.return_value = (
            klio_message,
            message_handler.MESSAGE_STATE.PROCESS,
        )

    mock_postprocess_klio_message = mocker.patch.object(
        message_handler, "postprocess_klio_message"
    )

    mock_process = mocker.Mock()

    def process(*args, **kwargs):
        mock_process.return_value = "should I stay or should I go"
        mock_process(*args, **kwargs)
        return mock_process.return_value

    wrapped_process = message_handler.parse_klio_message(process)

    try:
        next(wrapped_process(dofn_inst, "foo"))
    except StopIteration:
        pass

    mock_preprocess_klio_message.assert_called_once_with(dofn_inst, "foo")
    if is_dropped or preproc_raises:
        mock_postprocess_klio_message.assert_not_called()
    else:
        mock_postprocess_klio_message.assert_called_once_with(
            dofn_inst, klio_message
        )


@pytest.mark.parametrize(
    "msg_state,mock_process_called",
    (
        (message_handler.MESSAGE_STATE.PROCESS, True),
        (message_handler.MESSAGE_STATE.DROP, False),
    ),
)
def test_parse_klio_message_yields_returns(
    user_dofn,
    klio_message,
    klio_config,
    mocker,
    monkeypatch,
    msg_state,
    mock_process_called,
):
    dofn_inst = user_dofn()
    mock_config = mocker.Mock()
    mock_config.return_value = klio_config
    monkeypatch.setattr(
        core._KlioNamespace, "_load_config_from_file", mock_config
    )
    dofn_inst._klio.config.job_config.timeout_threshold = 0
    dofn_inst._klio.config.job_config.number_of_retries = 0

    mock_preprocess_klio_message = mocker.patch.object(
        message_handler, "preprocess_klio_message"
    )
    mock_preprocess_klio_message.return_value = (klio_message, msg_state)
    mock_postprocess_klio_message = mocker.patch.object(
        message_handler, "postprocess_klio_message"
    )
    mock_postprocess_klio_message.return_value = klio_message

    mock_process = mocker.Mock()
    mock_process.__name__ = "process"  # needed for py27 for functools.wraps
    mock_process.side_effect = iter("foobar")
    wrapped_process = message_handler.parse_klio_message(mock_process)

    ret = wrapped_process(dofn_inst, klio_message)

    assert isinstance(ret, types.GeneratorType)

    actual_ret_value = list(ret)

    mock_preprocess_klio_message.assert_called_once_with(
        dofn_inst, klio_message
    )

    if mock_process_called:
        actual_msg = klio_pb2.KlioMessage()
        actual_msg.ParseFromString(actual_ret_value[0])
        assert klio_message == actual_msg
        mock_process.assert_called_once_with(
            dofn_inst, klio_message.data.v1.entity_id
        )
        mock_postprocess_klio_message.assert_called_once_with(
            dofn_inst, klio_message
        )
    else:
        assert actual_ret_value == []
        mock_process.assert_not_called()
        mock_postprocess_klio_message.assert_not_called()

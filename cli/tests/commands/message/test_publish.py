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

import functools

import pytest

from google.api_core import exceptions as gapi_exceptions

from klio_core import config
from klio_core.proto.v1beta1 import klio_pb2

from klio_cli.commands.message import publish


@pytest.fixture
def mock_publisher(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(publish.pubsub, "PublisherClient", mock)
    return mock


@pytest.fixture
def klio_job_config():
    conf = {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {"project": "test-gcp-project"},
        "job_config": {
            "events": {
                "inputs": [
                    {
                        "type": "pubsub",
                        "topic": "an-input-topic",
                        "subscription": "a-subscription",
                    }
                ],
                "outputs": [{"type": "pubsub", "topic": "foo-topic-output"}],
            },
            "data": {
                "inputs": [
                    {"type": "gcs", "location": "gs://a-test-input/location"}
                ],
                "outputs": [
                    {"type": "gcs", "location": "foo-output-location"}
                ],
            },
        },
    }
    return config.KlioConfig(conf)


@pytest.fixture
def expected_klio_job(klio_job_config):
    klio_job = klio_pb2.KlioJob()
    klio_job.job_name = "test-job"
    klio_job.gcp_project = "test-gcp-project"
    return klio_job


@pytest.fixture
def expected_klio_message(expected_klio_job):
    message = klio_pb2.KlioMessage()
    message.metadata.downstream.extend([expected_klio_job])
    return message


def test_create_publisher(mock_publisher):
    client = mock_publisher.return_value

    ret_publisher = publish._create_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.get_topic.assert_called_once_with(request={"topic": "a-topic"})

    expected = functools.partial(client.publish, topic="a-topic")
    assert expected.func == ret_publisher.func


def test_create_publisher_topic_not_found(mock_publisher):
    client = mock_publisher.return_value
    client.get_topic.side_effect = gapi_exceptions.NotFound("foo")

    with pytest.raises(SystemExit):
        publish._create_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.get_topic.assert_called_once_with(request={"topic": "a-topic"})


def test_create_publisher_raises(mock_publisher):
    client = mock_publisher.return_value
    client.get_topic.side_effect = Exception("foo")

    with pytest.raises(Exception, match="foo"):
        publish._create_publisher("a-topic")

    mock_publisher.assert_called_once_with()
    client.get_topic.assert_called_once_with(request={"topic": "a-topic"})


def test_get_current_klio_job(klio_job_config, expected_klio_job):
    ret_job = publish._get_current_klio_job(klio_job_config)
    assert expected_klio_job == ret_job


@pytest.mark.parametrize(
    "force,ping,top_down,version",
    (
        (True, True, True, 1),
        (True, False, False, 1),
        (False, True, False, 1),
        (False, False, False, 1),
        (False, False, False, 2),
    ),
)
def test_create_pubsub_message(
    force, ping, top_down, version, expected_klio_job
):
    entity_id = "s0m3-ent1ty-1D"
    expected_klio_message = klio_pb2.KlioMessage()
    expected_klio_message.metadata.force = force
    expected_klio_message.metadata.ping = ping
    expected_klio_message.version = version
    if version == 1:
        expected_klio_message.data.entity_id = entity_id
        if not top_down:
            expected_klio_message.metadata.downstream.extend(
                [expected_klio_job]
            )
    else:
        expected_klio_message.data.element = bytes(entity_id, "utf-8")
        if not top_down:
            rec = expected_klio_message.metadata.intended_recipients
            rec.limited.recipients.extend([expected_klio_job])

    ret_msg = publish._create_pubsub_message(
        entity_id, expected_klio_job, force, ping, top_down, version
    )

    assert expected_klio_message.SerializeToString() == ret_msg


@pytest.mark.parametrize(
    "force,ping,top_down,non_klio",
    (
        (True, True, True, False),
        (True, False, False, False),
        (False, True, False, False),
        (False, False, False, False),
        (False, False, True, True),
    ),
)
def test_private_publish_messages(
    klio_job_config,
    mock_publisher,
    expected_klio_job,
    force,
    ping,
    top_down,
    non_klio,
):
    entity_id = "s0m3-ent1ty-1D"
    msg_version = 1
    if non_klio:
        exp_data = bytes(entity_id.encode("utf-8"))
    else:
        expected_klio_message = klio_pb2.KlioMessage()
        expected_klio_message.metadata.force = force
        expected_klio_message.metadata.ping = ping
        expected_klio_message.data.entity_id = entity_id
        expected_klio_message.version = msg_version
        if not top_down:
            expected_klio_message.metadata.downstream.extend(
                [expected_klio_job]
            )
        exp_data = expected_klio_message.SerializeToString()

    ret_success, ret_fail = publish._publish_messages(
        klio_job_config,
        [entity_id],
        force,
        ping,
        top_down,
        non_klio,
        msg_version,
    )

    mock_publisher.return_value.publish.assert_called_once_with(
        topic="an-input-topic", data=exp_data
    )

    assert 1 == len(ret_success)
    assert not len(ret_fail)


def test_private_publish_messages_raises(
    mock_publisher, klio_job_config, caplog
):
    client = mock_publisher.return_value
    client.publish.side_effect = Exception("foo")

    ret_success, ret_fail = publish._publish_messages(
        klio_job_config, ["s0m3-ent1ty-1D"], True, False, False, False, 1
    )

    assert not len(ret_success)
    assert 1 == len(ret_fail)
    assert 1 == len(caplog.records)


def test_publish_messages(
    klio_job_config, expected_klio_message, mock_publisher, caplog
):
    entity_id = "s0m3-ent1ty-1D"
    msg_version = 1
    expected_klio_message.metadata.force = False
    expected_klio_message.metadata.ping = False
    expected_klio_message.data.entity_id = entity_id
    expected_klio_message.version = msg_version

    publish.publish_messages(klio_job_config, [entity_id])

    mock_publisher.return_value.publish.assert_called_once_with(
        data=expected_klio_message.SerializeToString(), topic="an-input-topic"
    )
    assert 2 == len(caplog.records)

    assert "INFO" == caplog.records[0].levelname
    assert "INFO" == caplog.records[1].levelname


def test_publish_messages_fails(
    klio_job_config, expected_klio_message, mock_publisher, caplog
):
    client = mock_publisher.return_value
    client.publish.side_effect = Exception("foo")

    entity_id = "s0m3-ent1ty-1D"
    msg_version = 1
    expected_klio_message.metadata.force = False
    expected_klio_message.metadata.ping = False
    expected_klio_message.data.entity_id = entity_id
    expected_klio_message.version = msg_version

    publish.publish_messages(klio_job_config, [entity_id])

    mock_publisher.return_value.publish.assert_called_once_with(
        data=expected_klio_message.SerializeToString(), topic="an-input-topic"
    )
    assert 3 == len(caplog.records)
    assert "INFO" == caplog.records[0].levelname
    assert "WARNING" == caplog.records[1].levelname
    assert "WARNING" == caplog.records[2].levelname


def test_publish_messages_raises(klio_job_config, monkeypatch, caplog):
    monkeypatch.setattr(klio_job_config.job_config.events, "inputs", None)

    with pytest.raises(SystemExit):
        publish.publish_messages(klio_job_config, ["s0m3-ent1ty-1D"])

    assert 1 == len(caplog.records)

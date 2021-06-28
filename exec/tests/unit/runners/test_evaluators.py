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

"""Tests for KlioPubSubReadEvaluator

These were adapted from
apache_beam/io/gcp/pubsub_test.py::TestReadFromPubSub.

These validate that the original expected behavior from _PubSubReadEvaluator
was kept, as well checking that:
    * Responses are not auto-acked
    * MessageManager daemon threads started
    * Messages added to MessageManager
    * Messages handled one at a time, instead of 10 at a time
"""

import pytest

from apache_beam import transforms as beam_transforms
from apache_beam import utils as beam_utils
from apache_beam.io.gcp import pubsub as b_pubsub
from apache_beam.options import pipeline_options
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.direct import transform_evaluator
from apache_beam.testing import test_pipeline as beam_test_pipeline
from apache_beam.testing import test_utils as beam_test_utils
from apache_beam.testing import util as beam_testing_util

from klio.message import pubsub_message_manager as pmm
from klio_core.proto import klio_pb2

from klio_exec.runners import evaluators


@pytest.fixture
def patch_msg_manager(mocker, monkeypatch):
    p = mocker.Mock(name="patch_msg_manager")
    monkeypatch.setattr(pmm, "MessageManager", p)
    return p


@pytest.fixture
def patch_sub_client(mocker, monkeypatch):
    # patch out network calls in SubscriberClient instantiation
    c = mocker.Mock(name="patch_sub_client")
    monkeypatch.setattr(evaluators.g_pubsub, "SubscriberClient", c)
    return c.return_value


class KlioTestPubSubReadEvaluator(object):
    """Wrapper of _PubSubReadEvaluator that makes it bounded."""

    _pubsub_read_evaluator = evaluators.KlioPubSubReadEvaluator

    def __init__(self, *args, **kwargs):
        self._evaluator = self._pubsub_read_evaluator(*args, **kwargs)

    def start_bundle(self):
        return self._evaluator.start_bundle()

    def process_element(self, element):
        return self._evaluator.process_element(element)

    def finish_bundle(self):
        result = self._evaluator.finish_bundle()
        result.unprocessed_bundles = []
        result.keyed_watermark_holds = {None: None}
        return result


transform_evaluator.TransformEvaluatorRegistry._test_evaluators_overrides = {
    direct_runner._DirectReadFromPubSub: KlioTestPubSubReadEvaluator,
}


def test_klio_pubsub_read_eval_read_messages_success(
    mocker, patch_sub_client, patch_msg_manager,
):

    exp_entity_id = "entity_id"
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(exp_entity_id, "utf-8")
    data = kmsg.SerializeToString()

    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    attributes = {"key": "value"}
    ack_id = "ack_id"
    pull_response = beam_test_utils.create_pull_response(
        [
            beam_test_utils.PullResponseMessage(
                data, attributes, publish_time_secs, publish_time_nanos, ack_id
            )
        ]
    )
    pmsg = b_pubsub.PubsubMessage(data, attributes)
    expected_elements = [
        beam_testing_util.TestWindowedValue(
            pmsg,
            beam_utils.timestamp.Timestamp(1520861821.234567),
            [beam_transforms.window.GlobalWindow()],
        )
    ]
    patch_sub_client.pull.return_value = pull_response

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True
    with beam_test_pipeline.TestPipeline(options=options) as p:
        pcoll = p | b_pubsub.ReadFromPubSub(
            "projects/fakeprj/topics/a_topic", None, None, with_attributes=True
        )
        beam_testing_util.assert_that(
            pcoll,
            beam_testing_util.equal_to(expected_elements),
            reify_windows=True,
        )

    # Check overridden functionality:
    # 1. Check that auto-acking is skipped
    patch_sub_client.acknowledge.assert_not_called()
    # 2. Check that MessageManager daemon threads were started
    patch_msg_manager.assert_called_once_with(
        patch_sub_client.subscription_path()
    )
    # 3. Check that messages were added to the MessageManager
    patch_msg_manager.return_value.add.assert_called_once_with(ack_id, pmsg)
    # 4. Check that one message is handled at a time, instead of the
    #    original 10
    patch_sub_client.pull.assert_called_once_with(
        mocker.ANY, max_messages=1, return_immediately=True
    )

    patch_sub_client.api.transport.channel.close.assert_called_once_with()


def test_read_messages_timestamp_attribute_milli_success(
    mocker, patch_sub_client, patch_msg_manager,
):
    exp_entity_id = "entity_id"
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(exp_entity_id, "utf-8")
    data = kmsg.SerializeToString()

    attributes = {"time": "1337"}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    ack_id = "ack_id"
    pull_response = beam_test_utils.create_pull_response(
        [
            beam_test_utils.PullResponseMessage(
                data, attributes, publish_time_secs, publish_time_nanos, ack_id
            )
        ]
    )
    pmsg = b_pubsub.PubsubMessage(data, attributes)
    expected_elements = [
        beam_testing_util.TestWindowedValue(
            pmsg,
            beam_utils.timestamp.Timestamp(
                micros=int(attributes["time"]) * 1000
            ),
            [beam_transforms.window.GlobalWindow()],
        ),
    ]
    patch_sub_client.pull.return_value = pull_response

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True
    with beam_test_pipeline.TestPipeline(options=options) as p:
        pcoll = p | b_pubsub.ReadFromPubSub(
            "projects/fakeprj/topics/a_topic",
            None,
            None,
            with_attributes=True,
            timestamp_attribute="time",
        )
        # Check original functionality that was kept the same
        beam_testing_util.assert_that(
            pcoll,
            beam_testing_util.equal_to(expected_elements),
            reify_windows=True,
        )

    # Check overridden functionality:
    # 1. Check that auto-acking is skipped
    patch_sub_client.acknowledge.assert_not_called()
    # 2. Check that MessageManager daemon threads were started
    patch_msg_manager.assert_called_once_with(
        patch_sub_client.subscription_path()
    )
    # 3. Check that messages were added to the MessageManager
    patch_msg_manager.return_value.add.assert_called_once_with(ack_id, pmsg)
    # 4. Check that one message is handled at a time, instead of the
    #    original 10
    patch_sub_client.pull.assert_called_once_with(
        mocker.ANY, max_messages=1, return_immediately=True
    )

    patch_sub_client.api.transport.channel.close.assert_called_once_with()


def test_read_messages_timestamp_attribute_rfc3339_success(
    mocker, patch_sub_client, patch_msg_manager,
):
    exp_entity_id = "entity_id"
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(exp_entity_id, "utf-8")
    data = kmsg.SerializeToString()
    attributes = {"time": "2018-03-12T13:37:01.234567Z"}
    publish_time_secs = 1337000000
    publish_time_nanos = 133700000
    ack_id = "ack_id"
    pull_response = beam_test_utils.create_pull_response(
        [
            beam_test_utils.PullResponseMessage(
                data, attributes, publish_time_secs, publish_time_nanos, ack_id
            )
        ]
    )
    pmsg = b_pubsub.PubsubMessage(data, attributes)
    expected_elements = [
        beam_testing_util.TestWindowedValue(
            pmsg,
            beam_utils.timestamp.Timestamp.from_rfc3339(attributes["time"]),
            [beam_transforms.window.GlobalWindow()],
        ),
    ]
    patch_sub_client.pull.return_value = pull_response

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True
    with beam_test_pipeline.TestPipeline(options=options) as p:
        pcoll = p | b_pubsub.ReadFromPubSub(
            "projects/fakeprj/topics/a_topic",
            None,
            None,
            with_attributes=True,
            timestamp_attribute="time",
        )
        # Check original functionality that was kept the same
        beam_testing_util.assert_that(
            pcoll,
            beam_testing_util.equal_to(expected_elements),
            reify_windows=True,
        )

    # Check overridden functionality:
    # 1. Check that auto-acking is skipped
    patch_sub_client.acknowledge.assert_not_called()
    # 2. Check that MessageManager daemon threads were started
    patch_msg_manager.assert_called_once_with(
        patch_sub_client.subscription_path()
    )
    # 3. Check that messages were added to the MessageManager
    patch_msg_manager.return_value.add.assert_called_once_with(ack_id, pmsg)
    # 4. Check that one message is handled at a time, instead of the
    #    original 10
    patch_sub_client.pull.assert_called_once_with(
        mocker.ANY, max_messages=1, return_immediately=True
    )

    patch_sub_client.api.transport.channel.close.assert_called_once_with()


def test_read_messages_timestamp_attribute_missing(
    mocker, patch_sub_client, patch_msg_manager,
):
    exp_entity_id = "entity_id"
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(exp_entity_id, "utf-8")
    data = kmsg.SerializeToString()

    attributes = {}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    publish_time = "2018-03-12T13:37:01.234567Z"
    ack_id = "ack_id"
    pull_response = beam_test_utils.create_pull_response(
        [
            beam_test_utils.PullResponseMessage(
                data, attributes, publish_time_secs, publish_time_nanos, ack_id
            )
        ]
    )
    pmsg = b_pubsub.PubsubMessage(data, attributes)
    expected_elements = [
        beam_testing_util.TestWindowedValue(
            pmsg,
            beam_utils.timestamp.Timestamp.from_rfc3339(publish_time),
            [beam_transforms.window.GlobalWindow()],
        ),
    ]
    patch_sub_client.pull.return_value = pull_response

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True
    with beam_test_pipeline.TestPipeline(options=options) as p:
        pcoll = p | b_pubsub.ReadFromPubSub(
            "projects/fakeprj/topics/a_topic",
            None,
            None,
            with_attributes=True,
            timestamp_attribute="nonexistent",
        )
        # Check original functionality that was kept the same
        beam_testing_util.assert_that(
            pcoll,
            beam_testing_util.equal_to(expected_elements),
            reify_windows=True,
        )

    # Check overridden functionality:
    # 1. Check that auto-acking is skipped
    patch_sub_client.acknowledge.assert_not_called()
    # 2. Check that MessageManager daemon threads were started
    patch_msg_manager.assert_called_once_with(
        patch_sub_client.subscription_path()
    )
    # 3. Check that messages were added to the MessageManager
    patch_msg_manager.return_value.add.assert_called_once_with(ack_id, pmsg)
    # 4. Check that one message is handled at a time, instead of the
    #    original 10
    patch_sub_client.pull.assert_called_once_with(
        mocker.ANY, max_messages=1, return_immediately=True
    )

    patch_sub_client.api.transport.channel.close.assert_called_once_with()


def test_read_messages_timestamp_attribute_fail_parse(patch_sub_client):
    exp_entity_id = "entity_id"
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = bytes(exp_entity_id, "utf-8")
    data = kmsg.SerializeToString()

    attributes = {"time": "1337 unparseable"}
    publish_time_secs = 1520861821
    publish_time_nanos = 234567000
    ack_id = "ack_id"
    pull_response = beam_test_utils.create_pull_response(
        [
            beam_test_utils.PullResponseMessage(
                data, attributes, publish_time_secs, publish_time_nanos, ack_id
            )
        ]
    )
    patch_sub_client.pull.return_value = pull_response

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True
    p = beam_test_pipeline.TestPipeline(options=options)
    _ = p | b_pubsub.ReadFromPubSub(
        "projects/fakeprj/topics/a_topic",
        None,
        None,
        with_attributes=True,
        timestamp_attribute="time",
    )
    with pytest.raises(ValueError, match=r"parse"):
        p.run()

    patch_sub_client.acknowledge.assert_not_called()
    patch_sub_client.api.transport.channel.close.assert_called_with()

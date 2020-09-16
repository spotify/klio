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

import apache_beam as beam
import pytest

from apache_beam.options import pipeline_options
from apache_beam.testing import test_pipeline

from klio_core.proto import klio_pb2

from klio.transforms import helpers


class BaseTest:
    def get_current_job(self):
        job = klio_pb2.KlioJob()
        job.job_name = "a-job"
        job.gcp_project = "not-a-real-project"
        return job

    def get_other_job(self):
        job = klio_pb2.KlioJob()
        job.job_name = "other"
        job.gcp_project = "not-a-real-project"
        return job

    def assert_actual_is_expected(self, actual, expected):
        assert actual == expected, "Expected PColl was not processed"

    def assert_not_processed(self, *args, **kwargs):
        # We're not expecting to get called, so if it does fail, then we're
        # getting pcolls that we're not expecting. It may also mean
        # any call to assert_actual_is_expected was not called
        assert False, "Received an unexpected PColl"


# TODO: remove me when migration to v2 is done
@pytest.mark.usefixtures("mock_config")
class TestV1ExecutionMode(BaseTest):
    """v1 messages are processed or dropped depending on top-down or bottom up.
    """

    def prep_kmsg(self):
        msg = klio_pb2.KlioMessage()
        msg.data.entity_id = "s0m3_tr4ck_1d"
        return msg

    # unversioned incoming message
    def input_v0_kmsg(self):
        msg = self.prep_kmsg()
        return msg

    def input_v1_kmsg(self):
        msg = self.input_v0_kmsg()
        msg.version = klio_pb2.Version.V1
        return msg

    def exp_kmsg(self):
        msg = self.input_v1_kmsg()
        msg.data.element = b"s0m3_tr4ck_1d"
        return msg.SerializeToString()

    def test_top_down(self):
        pcoll = [
            self.input_v1_kmsg().SerializeToString(),
            self.input_v0_kmsg().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v1 | helpers._KlioV1CheckRecipients()

            _ = (
                should_process.process
                | "Assert expected processed"
                >> beam.Map(self.assert_actual_is_expected, expected=exp_pcoll)
            )
            _ = should_process.drop | "Assert no drops" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v2 | "Assert no v2" >> beam.Map(
                self.assert_not_processed
            )

    def input_v0_kmsg_downstream(self):
        msg = self.input_v0_kmsg()
        msg.metadata.downstream.extend([self.get_current_job()])
        return msg

    def input_v1_kmsg_downstream(self):
        msg = self.input_v1_kmsg()
        msg.metadata.downstream.extend([self.get_current_job()])
        return msg

    def exp_kmsg_downstream(self):
        msg = self.input_v1_kmsg_downstream()
        msg.data.element = b"s0m3_tr4ck_1d"
        return msg.SerializeToString()

    def exp_v0_kmsg_downstream(self):
        msg = self.input_v1_kmsg_downstream()
        msg.data.element = b"s0m3_tr4ck_1d"
        msg.version = klio_pb2.Version.V1
        return msg.SerializeToString()

    def test_bottom_up(self):
        pcoll = [
            self.input_v1_kmsg_downstream().SerializeToString(),
            self.input_v0_kmsg_downstream().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_downstream()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v1 | helpers._KlioV1CheckRecipients()

            _ = (
                should_process.process
                | "Assert expected processed"
                >> beam.Map(self.assert_actual_is_expected, expected=exp_pcoll)
            )
            _ = should_process.drop | "Assert no dropped msgs" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v2 | "Assert no v2 msgs" >> beam.Map(
                self.assert_not_processed
            )

    def input_v0_kmsg_drop(self):
        msg = self.input_v0_kmsg()
        msg.metadata.downstream.extend([self.get_other_job()])
        return msg

    def input_v1_kmsg_drop(self):
        msg = self.input_v1_kmsg()
        msg.metadata.downstream.extend([self.get_other_job()])
        return msg

    def exp_kmsg_drop(self):
        msg = self.input_v1_kmsg_drop()
        msg.data.element = b"s0m3_tr4ck_1d"
        return msg.SerializeToString()

    def test_bottom_up_drop(self):
        pcoll = [
            self.input_v1_kmsg_drop().SerializeToString(),
            self.input_v0_kmsg_drop().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_drop()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v1 | helpers._KlioV1CheckRecipients()

            _ = should_process.drop | "Assert expected dropped" >> beam.Map(
                self.assert_actual_is_expected, expected=exp_pcoll
            )
            _ = should_process.process | "Assert not processed" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v2 | "Assert no v2" >> beam.Map(
                self.assert_not_processed
            )


@pytest.mark.usefixtures("mock_config")
class TestExecutionMode(BaseTest):
    """Messages are processed or dropped depending on top-down or bottom up.
    """

    def prep_kmsg(self):
        msg = klio_pb2.KlioMessage()
        msg.data.element = b"s0m3_tr4ck_1d"
        return msg

    # unversioned incoming message
    def input_v0_kmsg_anyone(self):
        msg = self.prep_kmsg()
        msg.metadata.intended_recipients.anyone.SetInParent()
        return msg

    def input_v2_kmsg_anyone(self):
        msg = self.input_v0_kmsg_anyone()
        msg.version = klio_pb2.Version.V2
        return msg

    def exp_kmsg_anyone(self):
        msg = self.input_v2_kmsg_anyone()
        return msg.SerializeToString()

    def test_top_down(self):
        pcoll = [
            self.input_v0_kmsg_anyone().SerializeToString(),
            self.input_v2_kmsg_anyone().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_anyone()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v2 | helpers.KlioCheckRecipients()

            _ = (
                should_process.process
                | "Assert expected processed"
                >> beam.Map(self.assert_actual_is_expected, expected=exp_pcoll)
            )
            _ = should_process.drop | "Assert no dropped msgs" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v1 | "Assert no v1 msgs" >> beam.Map(
                self.assert_not_processed
            )

    def input_v0_kmsg_limited(self):
        msg = self.prep_kmsg()
        msg.metadata.intended_recipients.limited.recipients.extend(
            [self.get_current_job()]
        )
        return msg

    def input_v2_kmsg_limited(self):
        msg = self.input_v0_kmsg_limited()
        msg.version = klio_pb2.Version.V2
        return msg

    def exp_kmsg_limited(self):
        msg = self.input_v2_kmsg_limited()
        return msg.SerializeToString()

    def test_bottom_up(self):
        pcoll = [
            self.input_v0_kmsg_limited().SerializeToString(),
            self.input_v2_kmsg_limited().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_limited()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v2 | helpers.KlioCheckRecipients()

            _ = (
                should_process.process
                | "Assert expected processed"
                >> beam.Map(self.assert_actual_is_expected, expected=exp_pcoll)
            )
            _ = should_process.drop | "Assert no dropped msgs" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v1 | "Assert no v1 msgs" >> beam.Map(
                self.assert_not_processed
            )

    def input_v0_kmsg_drop(self):
        msg = self.prep_kmsg()
        msg.metadata.intended_recipients.limited.recipients.extend(
            [self.get_other_job()]
        )
        return msg

    def input_v2_kmsg_drop(self):
        msg = self.input_v0_kmsg_drop()
        msg.version = klio_pb2.Version.V2
        return msg

    def exp_kmsg_drop(self):
        msg = self.input_v2_kmsg_drop()
        return msg.SerializeToString()

    def test_bottom_up_drop(self):
        pcoll = [
            self.input_v0_kmsg_drop().SerializeToString(),
            self.input_v2_kmsg_drop().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_drop()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v2 | helpers.KlioCheckRecipients()

            _ = should_process.drop | "Assert expected dropped" >> beam.Map(
                self.assert_actual_is_expected, expected=exp_pcoll
            )
            _ = (
                should_process.process
                | "Assert no processed msgs"
                >> beam.Map(self.assert_not_processed)
            )
            _ = msg_version.v1 | "Assert no v1 msgs" >> beam.Map(
                self.assert_not_processed
            )

    def in_v0_kmsg_trigger_children_of(self):
        msg = self.prep_kmsg()
        current_job = self.get_current_job()
        lmtd = msg.metadata.intended_recipients.limited
        lmtd.recipients.extend([current_job, self.get_other_job()])
        lmtd.trigger_children_of.job_name = current_job.job_name
        lmtd.trigger_children_of.gcp_project = current_job.gcp_project
        return msg

    def in_v2_kmsg_trigger_children_of(self):
        msg = self.in_v0_kmsg_trigger_children_of()
        msg.version = klio_pb2.Version.V2
        return msg

    def exp_kmsg_trigger_children_of(self):
        msg = self.prep_kmsg()
        msg.version = klio_pb2.Version.V2
        msg.metadata.intended_recipients.anyone.SetInParent()
        return msg.SerializeToString()

    def _prep_pipeline(self):
        pcoll = [
            self.in_v0_kmsg_trigger_children_of().SerializeToString(),
            self.in_v2_kmsg_trigger_children_of().SerializeToString(),
        ]
        pipeline = test_pipeline.TestPipeline()
        in_pcol = pipeline.apply(beam.Create(pcoll), pipeline)
        msg_version = in_pcol.apply(helpers._KlioTagMessageVersion())
        return msg_version

    def test_bottom_up_to_top_down(self):
        pcoll = [
            self.in_v0_kmsg_trigger_children_of().SerializeToString(),
            self.in_v2_kmsg_trigger_children_of().SerializeToString(),
        ]
        exp_pcoll = self.exp_kmsg_trigger_children_of()

        with test_pipeline.TestPipeline() as p:
            in_pcol = p | beam.Create(pcoll)
            msg_version = in_pcol | helpers._KlioTagMessageVersion()
            should_process = msg_version.v2 | helpers.KlioCheckRecipients()

            _ = (
                should_process.process
                | "Assert expected processed"
                >> beam.Map(self.assert_actual_is_expected, expected=exp_pcoll)
            )
            _ = should_process.drop | "Assert no dropped msgs" >> beam.Map(
                self.assert_not_processed
            )
            _ = msg_version.v1 | "Assert no v1 msgs" >> beam.Map(
                self.assert_not_processed
            )


def assert_audit(actual):
    job = klio_pb2.KlioJob()
    job.job_name = "a-job"
    job.gcp_project = "not-a-real-project"
    audit_log_item = klio_pb2.KlioJobAuditLogItem()
    audit_log_item.klio_job.CopyFrom(job)
    exp_msg = klio_pb2.KlioMessage()
    exp_msg.version = klio_pb2.Version.V2
    exp_msg.metadata.job_audit_log.extend([audit_log_item])
    expected = exp_msg.SerializeToString()

    assert expected == actual
    return actual


def test_update_klio_log(mocker, monkeypatch, caplog, mock_config):
    mock_ts = mocker.Mock()
    monkeypatch.setattr(klio_pb2.KlioJobAuditLogItem, "timestamp", mock_ts)

    kmsg = klio_pb2.KlioMessage()
    kmsg.version = klio_pb2.Version.V2
    assert not kmsg.metadata.job_audit_log  # sanity check

    with test_pipeline.TestPipeline() as p:
        in_pcol = p | beam.Create([kmsg.SerializeToString()])
        act_pcol = in_pcol | helpers.KlioUpdateAuditLog()
        _ = act_pcol | beam.Map(assert_audit)

    exp_log = (
        "KlioMessage full audit log - Entity ID:  - Path: not-a-real-project::"
        "a-job (current job)"
    )
    for rec in caplog.records:
        if exp_log in rec.message:
            assert True
            break
    else:
        assert False, "Expected debug audit log not found"


def test_trigger_upstream_job(mock_config, mocker):
    mock_gcs_client = mocker.patch("klio.transforms._helpers.gcsio.GcsIO")
    mock_gcs_client.return_value.exists.return_value = False
    mock_pubsub_client = mocker.patch("google.cloud.pubsub.PublisherClient")

    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = b"does_not_exist"

    exp_current_job = klio_pb2.KlioJob()
    exp_current_job.job_name = "a-job"
    exp_current_job.gcp_project = "not-a-real-project"
    exp_upstream_job = klio_pb2.KlioJob()
    exp_upstream_job.job_name = "upstream-job"
    exp_upstream_job.gcp_project = "upstream-project"
    exp_kmsg = klio_pb2.KlioMessage()
    exp_kmsg.version = klio_pb2.Version.V2
    exp_kmsg.data.element = b"does_not_exist"
    exp_lmtd = exp_kmsg.metadata.intended_recipients.limited
    exp_lmtd.recipients.extend([exp_upstream_job, exp_current_job])
    exp_lmtd.trigger_children_of.CopyFrom(exp_current_job)

    options = pipeline_options.PipelineOptions([])
    options.view_as(pipeline_options.StandardOptions).streaming = True

    with test_pipeline.TestPipeline(options=options) as p:
        in_pcol = p | beam.Create([kmsg.SerializeToString()])
        input_data = in_pcol | helpers.KlioGcsCheckInputExists()

        _ = input_data.not_found | helpers.KlioTriggerUpstream(
            upstream_job_name="upstream-job",
            upstream_topic="projects/upstream-project/topics/does-not-exist",
        )

    mock_gcs_client.return_value.exists.assert_called_once_with(
        "gs://hopefully-this-bucket-doesnt-exist/does_not_exist"
    )
    mock_pubsub_client.return_value.publish.assert_called_once_with(
        mock_pubsub_client.return_value.topic_path.return_value,
        exp_kmsg.SerializeToString(),
    )

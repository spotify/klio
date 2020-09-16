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

import apache_beam as beam

from apache_beam import pvalue

from klio_core.proto import klio_pb2

from klio.message import serializer
from klio.transforms import _helpers
from klio.transforms import decorators
from klio.transforms import io as io_transforms


class KlioGcsCheckInputExists(
    _helpers._KlioInputDataMixin, _helpers._KlioGcsCheckExistsBase
):
    """Klio transform to check input exists in GCS."""

    pass


class KlioGcsCheckOutputExists(
    _helpers._KlioOutputDataMixin, _helpers._KlioGcsCheckExistsBase
):
    """Klio transform to check output exists in GCS."""

    pass


class KlioFilterPing(
    _helpers._KlioInputDataMixin, _helpers._KlioBaseDataExistenceCheck
):
    """Klio transform to tag outputs if in ping mode or not."""

    def ping(self, kmsg):
        global_ping = self._data_config.ping
        msg_ping = kmsg.metadata.ping
        return msg_ping if msg_ping else global_ping

    def process(self, kmsg):
        tagged_state = _helpers.TaggedStates.DEFAULT
        item = kmsg.data.element.decode("utf-8")

        if self.ping(kmsg):
            self._klio.logger.info("Pass through '%s': Ping mode ON." % item)
            tagged_state = _helpers.TaggedStates.PASS_THRU

        else:
            self._klio.logger.debug("Process '%s': Ping mode OFF." % item)
            tagged_state = _helpers.TaggedStates.PROCESS

        yield pvalue.TaggedOutput(tagged_state.value, kmsg.SerializeToString())


class KlioFilterForce(
    _helpers._KlioOutputDataMixin, _helpers._KlioBaseDataExistenceCheck
):
    """Klio transform to tag outputs if in force mode or not."""

    def force(self, kmsg):
        global_force = self._data_config.force
        msg_force = kmsg.metadata.force
        return msg_force if msg_force else global_force

    def process(self, kmsg):
        tagged_state = _helpers.TaggedStates.DEFAULT
        item_path = self._get_absolute_path(kmsg.data.element)
        item = kmsg.data.element.decode("utf-8")

        if not self.force(kmsg):
            self._klio.logger.info(
                "Pass through '%s': Force mode OFF with output found at '%s'."
                % (item, item_path)
            )
            tagged_state = _helpers.TaggedStates.PASS_THRU

        else:
            self._klio.logger.info(
                "Process '%s': Force mode ON with output found at '%s'."
                % (item, item_path)
            )
            tagged_state = _helpers.TaggedStates.PROCESS

        yield pvalue.TaggedOutput(tagged_state.value, kmsg.SerializeToString())


class KlioWriteToEventOutput(beam.PTransform):
    """Klio composite transform to write to the configured event output."""

    # NOTE: hopefully we don't get an dict lookup errors since KlioConfig
    # should raise if given an unsupported event IO transform
    CONFNAME_TO_OUT_TRANSFORM = {
        "file": io_transforms.KlioWriteToText,
        "pubsub": beam.io.WriteToPubSub,
    }

    @property
    @decorators._set_klio_context
    def _event_config(self):
        # TODO: figure out how to support multiple outputs

        if len(self._klio.config.job_config.events.outputs) > 1:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "The `klio.transforms.helpers.KlioWriteToEventOutput` "
                "transform does not support multiple outputs configured in "
                "`klio-job.yaml::job_config.events.outputs`."
            )

        if len(self._klio.config.job_config.events.outputs) == 0:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "The `klio.transforms.helpers.KlioWriteToEventOutput` "
                "requires an event output to be configured in "
                "`klio-job.yaml::job_config.events.outputs`."
            )
        return self._klio.config.job_config.events.outputs[0]

    def expand(self, pcoll):
        transform_kls = self.CONFNAME_TO_OUT_TRANSFORM[self._event_config.name]

        kwargs = self._event_config.as_dict()
        return (
            pcoll
            | "Writing to '%s'" % self._event_config.name
            >> transform_kls(**kwargs)
        )


# NOTE: not doing much right now, but allows us to extend if need be
class KlioDrop(beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass):
    """Klio DoFn to log & drop a KlioMessage."""

    WITH_OUTPUTS = False

    @decorators._handle_klio
    def process(self, kmsg):
        self._klio.logger.info(
            "Dropping KlioMessage - can not process '%s' any further."
            % kmsg.element
        )
        return


# TODO: this should only be temporary and removed once v2 migration is done
class _KlioTagMessageVersion(
    beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass
):
    WITH_OUTPUTS = True

    def process(self, klio_message):
        # In batch, the read transform produces a KlioMessage. However, in
        # streaming, it's still bytes. And for some reason this isn't
        # pickleable when it's in its own transform.
        # TODO: maybe create a read/write klio pub/sub transform to do
        # this for us.
        if not isinstance(klio_message, klio_pb2.KlioMessage):
            klio_message = serializer.to_klio_message(klio_message)

        if klio_message.version == klio_pb2.Version.V2:
            yield pvalue.TaggedOutput("v2", klio_message.SerializeToString())
        else:
            yield pvalue.TaggedOutput("v1", klio_message.SerializeToString())


# TODO: this should only be temporary and removed once v2 migration is done
class _KlioV1CheckRecipients(
    beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass
):
    """Check if current job should handle a received v1 message."""

    WITH_OUTPUTS = True

    @decorators._set_klio_context
    def _should_process(self, klio_message):
        downstream = klio_message.metadata.downstream
        if not downstream:
            # if there's nothing in downstream, then it means the message is
            # in top-down mode and should be handled
            return True

        current_job = klio_pb2.KlioJob()
        current_job.ParseFromString(self._klio.job)

        if _helpers._job_in_jobs(current_job, downstream):
            return True

        self._klio.logger.info(
            "Dropping KlioMessage - job not an intended recipient for message "
            "with entity_id {}.".format(klio_message.data.entity_id)
        )
        return False

    def process(self, raw_message):
        klio_message = serializer.to_klio_message(raw_message)
        if self._should_process(klio_message):
            yield pvalue.TaggedOutput(
                _helpers.TaggedStates.PROCESS.value, raw_message
            )
        else:
            yield pvalue.TaggedOutput(
                _helpers.TaggedStates.DROP.value, raw_message
            )


class KlioCheckRecipients(
    beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass
):
    """Check if current job should handle a received v2 message."""

    WITH_OUTPUTS = True

    @decorators._set_klio_context
    def _should_process(self, klio_message):
        intended_recipients = klio_message.metadata.intended_recipients
        # returns "anyone", "limited", or None if not set
        recipients = intended_recipients.WhichOneof("recipients")

        if recipients is None:
            # is it safe to assume if this is not set in a v2 message, it should
            # be top-down? I think this will be the case for batch
            self._klio.logger.warning(
                "Dropping KlioMessage - No 'intended_recipients' set in "
                "metadata of KlioMessage with element '{}'.".format(
                    klio_message.data.element
                )
            )
            return False

        if recipients == "anyone":
            return True

        current_job = klio_pb2.KlioJob()
        current_job.ParseFromString(self._klio.job)

        # otherwise, recipients == "limited"
        # don't process if this job is not in the intended recipients
        if not _helpers._job_in_jobs(
            current_job, intended_recipients.limited.recipients
        ):
            return False

        # if it is in the intended recipients _and_ is the job in
        # trigger_children_of, then this message was originally in top-down
        # mode, but was missing dependencies, and therefore should update the
        # message intended receipients to be "anyone" signifying top-down
        if _helpers._job_in_jobs(
            current_job, [intended_recipients.limited.trigger_children_of]
        ):
            # FYI: since 'anyone' is essentially empty msg, it can't simply
            # be assigned. To set `anyone` as the intended_recipients, use
            # kmsg.metadata.intended_recipients.anyone.SetInParent()`
            # https://stackoverflow.com/a/29651069
            intended_recipients.anyone.SetInParent()

        return True

    def process(self, raw_message):
        klio_message = serializer.to_klio_message(raw_message)
        if self._should_process(klio_message):
            # the message could have updated, so let's re-serialize to a new
            # raw message
            raw_message = klio_message.SerializeToString()
            yield pvalue.TaggedOutput(
                _helpers.TaggedStates.PROCESS.value, raw_message
            )
        else:
            yield pvalue.TaggedOutput(
                _helpers.TaggedStates.DROP.value, raw_message
            )


class KlioUpdateAuditLog(beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass):
    """Update a KlioMessage's audit log to include current job."""

    WITH_OUTPUTS = False

    @decorators._set_klio_context
    def _generate_current_job_object(self):
        job = klio_pb2.KlioJob()
        job.job_name = self._klio.config.job_name
        job.gcp_project = self._klio.config.pipeline_options.project
        return job

    def _create_audit_item(self):
        audit_log_item = klio_pb2.KlioJobAuditLogItem()
        audit_log_item.timestamp.GetCurrentTime()
        current_job = self._generate_current_job_object()
        audit_log_item.klio_job.CopyFrom(current_job)
        return audit_log_item

    @decorators._set_klio_context
    def process(self, raw_message):
        klio_message = serializer.to_klio_message(raw_message)
        audit_log_item = self._create_audit_item()
        klio_message.metadata.job_audit_log.extend([audit_log_item])

        audit_log = klio_message.metadata.job_audit_log
        traversed_dag = " -> ".join(
            "{}::{}".format(
                str(al.klio_job.gcp_project), str(al.klio_job.job_name)
            )
            for al in audit_log
        )
        traversed_dag = "{} (current job)".format(traversed_dag)

        base_log_msg = "KlioMessage full audit log"
        log_msg = "{} - Entity ID: {} - Path: {}".format(
            base_log_msg, klio_message.data.entity_id, traversed_dag
        )
        self._klio.logger.debug(log_msg)
        yield klio_message.SerializeToString()


class KlioDebugMessage(beam.PTransform):
    """Log KlioMessage.

    Args:
        prefix (str): logging prefix. Default: ``"DEBUG"``.
        log_level (str or int): The desired log level for the KlioMessage
            logs. See `available log levels <https://docs.python.org/3/library/
            logging.html#levels>`_ for what's supported. Default: ``"INFO"``.
    """

    def __init__(self, prefix="DEBUG: ", log_level="INFO"):
        super().__init__()
        self.prefix = prefix
        self.log_level = self._get_log_level(log_level)

    def _get_log_level(self, log_level):
        # TODO: should prob do some pre-emptive checking
        if isinstance(log_level, str):
            return getattr(logging, log_level.upper())
        if isinstance(log_level, int):
            return log_level
        raise SystemExit("Unrecognized `log_level` for `KlioDebugMessage`.")

    @decorators._set_klio_context
    def print_debug(self, raw_message):
        klio_message = serializer.to_klio_message(raw_message)
        self._klio.logger.log(
            self.log_level, "{}{}".format(self.prefix, klio_message)
        )
        return raw_message

    def expand(self, pipeline):
        return pipeline | beam.Map(self.print_debug)


class KlioSetTrace(beam.PTransform):
    """Insert a Python debugger trace point."""

    def set_trace(self, raw_message):
        import pdb  # don't import this at top level, just when it's needed

        pdb.set_trace()
        return raw_message

    def expand(self, pipeline):
        return pipeline | beam.Map(self.set_trace)


class KlioTriggerUpstream(beam.PTransform):
    """Trigger upstream job from current job with a given ``KlioMessage``.

    This transform will update the intended recipients in ``KlioMessage.
    metadata`` in order to trigger a partial :ref:`bottom-up execution
    <bottom-up>` of the overall graph of jobs. It will also generate a log
    message (optional), then publish the ``KlioMessage`` to the upstream's
    Pub/Sub topic.

    .. caution::

        Klio does not automatically trigger upstream jobs if input data does
        not exist. It must be used manually within a job's pipeline definition
        (in ``run.py::run``).

    .. note::

        In order to get access to input data not found, the automatic data
        existence check that Klio does must be turned off by setting
        ``klio-job.yaml::job_config.data.inputs[].skip_klio_existence_check``
        to ``True``. Then the existence check must be invoked manually. See
        example ``run.py`` and ``klio-job.yaml`` files below.

    Example usage:

    .. code-block:: python

        import apache_beam as beam
        from klio.transforms import helpers
        import transforms

        def run(input_pcol, config):
            # use the default helper transform to do the default input check
            # in order to access the output tagged with `not_found`
            input_data = input_pcol | helpers.KlioGcsCheckInputExists()

            # Pipe the input data that was not found (using Tagged Outputs)
            # into `KlioTriggerUpstream` in order to update the KlioMessage
            # metadata, log it, then publish to upstream's
            _ = input_data.not_found | helpers.KlioTriggerUpstream(
                upstream_job_name="my-upstream-job",
                upstream_topic="projects/my-gcp-project/topics/upstream-topic",
                log_level="DEBUG",
            )

            # pipe the found input pcollection into transform(s) as needed
            output_pcol = input_data.found | beam.ParDo(MyTransform())
            return output_pcol

    .. code-block:: yaml
        :emphasize-lines: 7,23

        # Example klio-job.yaml
        version: 2
        job_name: my-job
        pipeline_options:
          project: my-gcp-project
          # `KlioTriggerUpstream` only supports streaming jobs
          streaming: True
          # <-- snip -->
        job_config:
          events:
            inputs:
              - type: pubsub
                topic: projects/my-gcp-project/topics/upstream-topic-output
                subscription: projects/my-gcp-project/subscriptions/my-job-in
            # <-- snip -->
          data:
            inputs:
              - type: gcs
                location: gs://my-gcp-project/upstream-output-data
                file_suffix: .ogg
                # Be sure to skip Klio's default input existence check in
                # order to access the input data that was not found.
                skip_klio_existence_check: True

    Args:
        upstream_job_name (str): Name of upstream job.
        upstream_topic (str): Pub/Sub topic for the upstream job, in the
            form of ``project/<GCP_PROJECT>/topics/<TOPIC_NAME>``.
        log_level (str, int, or None): The desired log level for log message,
            or ``None`` if no logging is desired. See `available log levels
            <https://docs.python.org/3/library/logging.html#levels>`_ for
            what's supported. Default: ``"INFO"``.

    Raises:
        SystemExit: If the current job is not in streaming mode (set
            in `klio-job.yaml::pipeline_options.streaming`), if the
            provided log level is not recognized, or if the provided
            upstream topic is not in the correct form.
    """

    @decorators._set_klio_context
    def __init__(self, upstream_job_name, upstream_topic, log_level="INFO"):
        if self._klio.config.pipeline_options.streaming is False:
            # Fail early
            self._klio.logger.error(
                "The `KlioTriggerUpstreams` transform is only available for "
                "jobs in streaming mode."
            )
            raise SystemExit(1)

        self.upstream_job_name = upstream_job_name
        self.upstream_topic = upstream_topic
        self.upstream_gcp_project = self._get_project_from_topic()
        self.log_level = self._get_log_level(log_level)

    def _get_project_from_topic(self):
        stems = self.upstream_topic.split("/")
        if len(stems) != 4:
            # Fail early
            self._klio.logger.error(
                "The provided upstream topic for `KlioTriggerUpstream` is "
                "expected in the form of 'project/<GCP_PROJECT>/topics/"
                "<TOPIC_NAME>'. Received '{}'.".format(self.upstream_topic)
            )
            raise SystemExit(1)
        return stems[1]

    def _get_log_level(self, log_level):
        if log_level is None:
            return log_level

        if isinstance(log_level, (str, int)):
            # save to a different variable so we don't alter it for the error
            # log message below
            _log_level = log_level
            if isinstance(_log_level, str):
                _log_level = _log_level.upper()

            level = logging.getLevelName(_log_level)
            if isinstance(level, int):
                return level

            # getLevelName will create a level if it doesn't recognize it (wtf)
            # so let's check if it actually exists
            ret = getattr(logging, level, None)
            if ret is not None:
                return ret
        self._klio.logger.error(
            "Unrecognized log level '{}' for `KlioTriggerUpstream`.".format(
                log_level
            )
        )
        raise SystemExit(1)

    def _generate_upstream_job_object(self):
        upstream_job = klio_pb2.KlioJob()
        upstream_job.job_name = self.upstream_job_name
        upstream_job.gcp_project = self.upstream_gcp_project
        return upstream_job

    def default_label(self):
        # Will be overwritten when invoked with a custom label, i.e.
        # `"Some Label" >> KlioTriggerUpstream(...)`
        return "{}(upstream={})".format(
            self.__class__.__name__, self.upstream_job_name
        )

    @decorators._set_klio_context
    def _generate_current_job_object(self):
        job = klio_pb2.KlioJob()
        job.job_name = self._klio.config.job_name
        job.gcp_project = self._klio.config.pipeline_options.project
        return job

    @decorators._set_klio_context
    def update_kmsg_metadata(self, raw_kmsg):
        """Update KlioMessage to enable partial bottom-up execution.

        Args:
            raw_kmsg (bytes): Unserialized KlioMessage
        Returns:
            bytes: KlioMessage deserialized to ``bytes`` with updated intended
                recipients metadata.
        """
        # Use `serializer.to_klio_message` instead of @handle_klio in order to
        # get the full KlioMessage object (not just the data).
        kmsg = serializer.to_klio_message(
            raw_kmsg, kconfig=self._klio.config, logger=self._klio.logger
        )

        # Make sure upstream job doesn't skip the message
        upstream_job = self._generate_upstream_job_object()
        lmtd = kmsg.metadata.intended_recipients.limited
        lmtd.recipients.extend([upstream_job])

        # Assign the current job to `trigger_children_of` so that top-down
        # execution resumes after this job is done.
        current_job = self._generate_current_job_object()
        lmtd.recipients.extend([current_job])
        lmtd.trigger_children_of.CopyFrom(current_job)
        return serializer.from_klio_message(kmsg)

    @decorators._handle_klio
    def log(self, data):
        """Log KlioMessage being published to upstream job.

        Args:
            data (klio_core.proto.KlioMessage.data): data of ``KlioMessage``.
        Returns:
            klio_core.proto.KlioMessage.data: unchanged data of
                ``KlioMessage``.
        """
        element = data.element.decode("utf-8")
        msg = "Triggering upstream {} for {}".format(
            self.upstream_job_name, element
        )
        self._klio.logger.log(self.log_level, msg)
        return data

    def expand(self, pcoll):
        name = self.upstream_job_name
        lbl1 = "Update KlioMessage for Upstream {}".format(name)
        lbl2 = "Log Triggering Upstream {}".format(name)
        lbl3 = "Publish KlioMessage to Upstream {}".format(name)

        updated_kmsg = pcoll | lbl1 >> beam.Map(self.update_kmsg_metadata)

        if self.log_level is not None:
            _ = updated_kmsg | lbl2 >> beam.Map(self.log)

        return updated_kmsg | lbl3 >> beam.io.WriteToPubSub(
            topic=self.upstream_topic
        )

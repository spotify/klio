# Copyright 2020 Spotify AB

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


# TODO: define a helper transform that can trigger parents of a streaming job.
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

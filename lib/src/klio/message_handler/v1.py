# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import functools
import sys
import types
import warnings

from google.api_core import exceptions as gapi_exceptions
from google.protobuf import message as gproto_message

from klio_core import utils
from klio_core.proto.v1beta1 import klio_pb2

from klio import timeout_process
from klio.message_handler import exceptions

if sys.version_info < (3,):  # pragma: no cover
    func_name = "func_name"
else:
    func_name = "__name__"  # pragma: no cover


MESSAGE_STATE = utils.enum(
    "PROCESS",  # message should be processed as normal
    "PASS_THRU",  # skip processing but publish message to output topic
    "DROP",  # message should be dropped entirely
)


def _job_in_jobs(current_job, job_list):
    # Checking only name & project because there may be incomplete
    # KlioJobs objects from adding parents into up/downstream.
    curr_job_name = "%s-%s" % (current_job.gcp_project, current_job.job_name)
    downstream_job_names = [
        "%s-%s" % (j.gcp_project, j.job_name) for j in job_list
    ]

    return curr_job_name in downstream_job_names


def postprocess_klio_message(dofn_inst, parsed_message):
    """Postprocess logic for a Klio message.

    This postprocess function contains logic for:
        - updating the list of visited jobs with this job's
          topic (for created a "paper trail");
        - re-serializing the protobuf message to be published to the
          job's output topic.

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
    Returns:
        (google.cloud.pubsub.types.PubsubMessage): a message to be
            published to Google Pub/Sub.
    """
    klio_job = klio_pb2.KlioJob()
    klio_job.ParseFromString(dofn_inst._klio.job)
    parsed_message.metadata.visited.extend([klio_job])

    if parsed_message.metadata.ping:
        # TODO: For now, we'll log when a message is in ping mode.
        #       In the future, we should either create custom metrics
        #       for Stackdriver for reporting ping mode (and in general),
        #       or figure out a user-friendly way to visualize the dag
        #       that ping mode constructs @lynn
        visited = parsed_message.metadata.visited
        traversed_dag = " -> ".join(
            "%s::%s" % (str(j.gcp_project), str(j.job_name)) for j in visited
        )
        traversed_dag = "%s (current job)" % traversed_dag

        base_log_msg = "KlioMessage received in 'ping' mode"
        log_msg = "%s - Entity ID: %s - Path: %s" % (
            base_log_msg,
            parsed_message.data.entity_id,
            traversed_dag,
        )
        dofn_inst._klio.logger.info(log_msg)

    return parsed_message


def trigger_parent_jobs(dofn_inst, parsed_message):
    """Trigger parent jobs via Pub/Sub.

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
    """
    if not dofn_inst._klio.parent_jobs:
        # TODO: add functionality to update parent jobs (either lazily or
        #       periodically) in case dataflow API issues on startup of job
        #       or parent job(s) are not currently running when current job
        #       starts up @lynn
        # TODO / idea: have stateful data on all job config (i.e. cloud sql,
        #              monorepo, DHT, etc) so that we're not reliant on
        #              parent jobs being up when querying dataflow API on
        #              startup. @lynn
        # TODO: if/when either or both of the above TODOs are addressed,
        #       then this warning is no longer needed. @lynn
        warn_msg = (
            "Input data for %s does not exist, but there are no parent jobs "
            "to trigger to create required input data.\n"
            "If there should be parent jobs, be sure there in the job's "
            "'klio-job.yaml' config file, and try restarting the job."
            % parsed_message.data.entity_id
        )
        warnings.warn(warn_msg, RuntimeWarning)
        return

    # TODO: add retry counts to a message (and allow it to be configurable)
    #       so that messages do not get stuck in a loop if for some reason
    #       upstream jobs can not produce required input data. @lynn
    for parent in dofn_inst._klio.parent_jobs:
        parent_job = klio_pb2.KlioJob()
        parent_job.ParseFromString(parent)
        if not _job_in_jobs(parent_job, parsed_message.metadata.downstream):
            # make sure parent job doesn't skip the message
            parsed_message.metadata.downstream.extend([parent_job])

        for job_input in parent_job.inputs:
            topic = str(job_input.topic)
            entity_id = parsed_message.data.entity_id
            try:
                publisher_client = utils.get_publisher(topic)
            except gapi_exceptions.GoogleAPIError as e:
                dofn_inst._klio.logger.error(
                    "Error creating publisher client for topic '%s': %s\n"
                    "Dropping message with Entity ID %s"
                    % (topic, e, entity_id)
                )
                return

            # TODO: try/except, if error, update upstream jobs, and try
            #       once more - but is it really needed, to update upstream
            #       jobs? we'd change config if that's needed
            # TODO: potential for lost messages if topic is up, but the
            #       parent job reading from topic is not (yet) up and
            #       subscribed. There should be at least a basic check to
            #       see if parent job is running. @lynn
            try:
                to_pubsub = parsed_message.SerializeToString()
                publisher_client.publish(topic, to_pubsub)
            except gapi_exceptions.GoogleAPIError as e:
                dofn_inst._klio.logger.error(
                    "Error publishing message to topic '%s': %s\n"
                    "Dropping message with Entity ID %s"
                    % (topic, e, entity_id)
                )
            else:
                dofn_inst._klio.logger.info(
                    "Triggered upstream job '%s' via topic '%s' for Entity "
                    "ID %s." % (parent_job.job_name, topic, entity_id)
                )


def check_input_data_exists(dofn_inst, parsed_message, current_job):
    """Check if expected input data exists for the message.

    If the input data does not exist, the message is dropped from this
    job, and the parent job(s) are triggered (bottom-up execution).

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
        current_job (klio_pb2.KlioJob): the currently-running job.
    Returns:
        MESSAGE_STATE.DROP if data doesn't exist, else
        MESSAGE_STATE.PROCESS
    """
    input_exists = dofn_inst.input_data_exists(parsed_message.data.entity_id)
    if not input_exists:
        dofn_inst._klio.logger.info(
            "Input data does not yet exist. Triggering parent jobs to "
            "produce missing input data."
        )
        if not _job_in_jobs(current_job, parsed_message.metadata.downstream):
            parsed_message.metadata.downstream.extend([current_job])
        trigger_parent_jobs(dofn_inst, parsed_message)
        return MESSAGE_STATE.DROP

    return MESSAGE_STATE.PROCESS


def check_output_data_exists(dofn_inst, parsed_message):
    """Check if the output data for the entity ID exists.

    If the output data does exist, and the message metadata has
     ``force`` set to ``False``, then the message will be skipped. If
    ``force`` is set to ``True``, the message will be processed whether
    or not the output data exists.

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
    Returns:
        None if the output data does exist and ``Force`` is set to
            ``False``, or the parsed_message (klio_pb2.KlioMessage) if
            output data does not exist, or ``Force`` is set to ``True``.
    """
    if parsed_message.metadata.force is True:
        return False

    output_exists = dofn_inst.output_data_exists(parsed_message.data.entity_id)
    if output_exists:
        dofn_inst._klio.logger.debug(
            "Skipping entity ID %s: output data already exists."
            % parsed_message.data.entity_id
        )
        return True

    return False


def update_audit_log(dofn_inst, parsed_message, current_job):
    """Update the message's audit log with the current job.

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        parsed_message (klio_pb2.KlioMessage): a Klio message.
        current_job (klio_pb2.KlioJob): the currently-running job.
    Returns:
        parsed_message (klio_pb2.KlioMessage): a Klio message with
            the audit log updated.
    """
    audit_log_item = klio_pb2.KlioJobAuditLogItem()
    audit_log_item.timestamp.GetCurrentTime()
    audit_log_item.klio_job.CopyFrom(current_job)
    parsed_message.metadata.job_audit_log.extend([audit_log_item])
    audit_log = parsed_message.metadata.job_audit_log
    traversed_dag = " -> ".join(
        "%s::%s" % (str(j.klio_job.gcp_project), str(j.klio_job.job_name))
        for j in audit_log
    )
    traversed_dag = "%s (current job)" % traversed_dag

    base_log_msg = "KlioMessage full audit log"
    log_msg = "%s - Entity ID: %s - Path: %s" % (
        base_log_msg,
        parsed_message.data.entity_id,
        traversed_dag,
    )
    dofn_inst._klio.logger.debug(log_msg)
    return parsed_message


def preprocess_klio_message(dofn_inst, incoming_message):
    """Preprocess logic for a Klio message.

    This preprocess function contains logic for:
        - deserializing the protobuf message within the consumed
          Pub/Sub message;
        - handling "force" mode if output data already exists;
        - handle parent job triggering if input data does not
          exist (bottom-up execution).

    Args:
        dofn_inst (beam.DoFn): User-implemented DoFn.
        incoming_message (google.cloud.pubsub.types.PubsubMessage): a
            message consumed from Google Pub/Sub.
    Returns:
        (tuple(klio_pb2.KlioMessage, MESSAGE_STATE)) the Klio message
            along with the status the message for how to handle the
            message.
    """
    message_state = MESSAGE_STATE.PROCESS

    # TODO: somewhere here is probably where we want to add support for
    #       loading custom protobuf schemas (@lynn)
    parsed_message = klio_pb2.KlioMessage()
    try:
        parsed_message.ParseFromString(incoming_message)
    # any other error is caught in `parse_klio_message`
    except gproto_message.DecodeError as e:
        if dofn_inst._klio.config.job_config.allow_non_klio_messages:
            # We are assuming that we have been given "raw" data that is not in
            # the form of a serialized KlioMessage. We're just going to treat it
            # as "data" (losing the benefits of top-down/bottom-up execution)
            if dofn_inst._klio.config.job_config.binary_non_klio_messages:
                parsed_message.data.payload = incoming_message
            else:
                parsed_message.data.entity_id = incoming_message
        else:
            dofn_inst._klio.logger.error(
                "Can not parse incoming message. To support non-Klio "
                "messages, add `job_config.allow_non_klio_messages = true` "
                "in the job's `klio-job.yaml` file."
            )
            raise e

    ver = parsed_message.version
    # backwards-compat support for messages w/o versions (no version
    # defaults to 1)
    if ver and ver is not klio_pb2.Version.V1:
        msg = (
            "Job is not configured to parse version '%s' of KlioMessage. "
            "Errored message: %s" % (parsed_message.version, parsed_message)
        )
        raise exceptions.KlioVersionMismatch(msg)

    # coerce older messages into versioned messages
    if parsed_message.data.entity_id:
        parsed_message.data.entity_id = parsed_message.data.entity_id

    downstream = parsed_message.metadata.downstream
    current_job = klio_pb2.KlioJob()
    current_job.ParseFromString(dofn_inst._klio.job)

    # if there are downstream jobs, and this job isn't in that array,
    # then drop message
    if downstream and not _job_in_jobs(current_job, downstream):
        dofn_inst._klio.logger.info(
            "Skipping KlioMessage - intended downstream jobs does not "
            "contain this job."
        )
        return parsed_message, MESSAGE_STATE.DROP

    parsed_message = update_audit_log(dofn_inst, parsed_message, current_job)

    if parsed_message.metadata.ping:
        return parsed_message, MESSAGE_STATE.PASS_THRU

    should_skip = check_output_data_exists(dofn_inst, parsed_message)
    if should_skip is True:
        return parsed_message, MESSAGE_STATE.PASS_THRU

    message_state = check_input_data_exists(
        dofn_inst, parsed_message, current_job
    )

    return parsed_message, message_state


def timeout_wrapped_process(
    dofn_inst, entity_id, process_method, n_retries, timeout_threshold
):
    mapped_result = dofn_inst._klio._thread_pool.starmap_async(
        retry_process,
        iterable=[
            (
                dofn_inst,
                entity_id,
                process_method,
                n_retries,
                timeout_threshold,
            )
        ],
    )

    while True:
        if mapped_result.ready():
            return mapped_result


def timeout_dofn_process(process, timeout_threshold, klio_logger, entity_id):
    klio_logger.debug(
        "Starting %s for KlioMessage with entity ID %s"
        % (process.name, entity_id)
    )
    process.start()
    klio_logger.debug(
        "Setting timeout of %s seconds for KlioMessage "
        "with entity ID %s" % (timeout_threshold, entity_id)
    )
    process.join(timeout_threshold)
    if process.exception:
        raise Exception(process.exception)
    if process.is_alive():
        process.terminate()
        raise Exception(
            "KlioTimeoutException - %s timed out while processing entity ID %s."
            % (process.name, entity_id)
        )


def retry_process(
    dofn_inst, entity_id, process_method, n_retries, timeout_threshold=0
):
    transform_name = dofn_inst._klio._transform_name
    while n_retries >= 0:
        try:
            if timeout_threshold > 0:
                p = timeout_process.TimeoutProcess(
                    target=process_method,
                    args=([dofn_inst, entity_id]),
                    name="TimeoutProcess_{}_{}".format(
                        transform_name, entity_id
                    ),
                )

                timeout_dofn_process(
                    p, timeout_threshold, dofn_inst._klio.logger, entity_id
                )
            else:
                ret = process_method(dofn_inst, entity_id)
                # if DoFn.process method `yield`s instead of `return`s
                if isinstance(ret, types.GeneratorType):
                    next(ret)
        except Exception as e:
            if n_retries == 0:
                dofn_inst._klio.logger.error(
                    "%s raised an exception while processing entity"
                    "ID %s. There are no more retries available."
                    % (transform_name, entity_id)
                )
                raise e
            logging_verb = "are" if n_retries > 1 else "is"
            logging_retry = "retries" if n_retries > 1 else "retry"
            dofn_inst._klio.logger.error(
                "Retrying KlioMessage - %s raised an exception while processing"
                " entity ID %s. There %s %s more %s available: %s"
                % (
                    transform_name,
                    entity_id,
                    logging_verb,
                    n_retries,
                    logging_retry,
                    e,
                )
            )
            n_retries -= 1
        else:
            return


def parse_klio_message(process_method):
    """Decorator to handle logic related to message metadata.

    This decorator contains logic for:
        - preprocessing for the klio message
        - dropping the message if no transform work should be done;
        - handling "ping" mode;
        - invoking the user-implemented DoFn process method with the
          message data (entity ID);
        - postprocessing for the klio message.

    Args:
        process_method (func): The `process` method as implemented in
            the user's DoFn.
    Returns:
        The wrapped function.
    """

    @functools.wraps(process_method)
    def wrapper(self, incoming_pubsub_msg, *args, **kwargs):
        to_yield = True

        try:
            parsed_msg, msg_state = preprocess_klio_message(
                self, incoming_pubsub_msg
            )
        except Exception as e:
            self._klio.logger.error(
                "Dropping KlioMessage - exception occurred when parsing "
                "consumed message %s: %s" % (incoming_pubsub_msg, e),
                exc_info=True,
            )
            msg_state = MESSAGE_STATE.DROP

        if msg_state is MESSAGE_STATE.DROP:
            to_yield = False

        if msg_state is MESSAGE_STATE.PROCESS:

            payload = None
            if parsed_msg.data.entity_id:
                payload = parsed_msg.data.entity_id
            else:
                payload = parsed_msg.data.payload

            n_retries = self._klio.config.job_config.number_of_retries
            timeout_threshold = self._klio.config.job_config.timeout_threshold
            try:
                if timeout_threshold > 0:
                    processed_result = timeout_wrapped_process(
                        self,
                        payload,
                        process_method,
                        n_retries,
                        timeout_threshold,
                    )
                    if not processed_result.successful():
                        self._klio.logger.error(
                            "Dropping KlioMessage - process reached number of "
                            "possible retries with a timeout for entity ID %s."
                            % (parsed_msg.data.entity_id)
                        )
                        to_yield = False
                elif n_retries:
                    retry_process(self, payload, process_method, n_retries)
                else:
                    # NOTE: users may return/yield the entity ID
                    #       (or potentially something else?!) from
                    #       their DoFn's process method. We're throwing
                    #       it away for now with assuming that the
                    #       input ID won't be different than the
                    #       output ID. We may want to handled
                    #       returned/yielded items post-MVP.
                    #       / @lynn
                    ret = process_method(self, payload)
                    # if DoFn.process method `yield`s instead of `return`s
                    if isinstance(ret, types.GeneratorType):
                        next(ret)
            except Exception as e:
                self._klio.logger.error(
                    "Dropping KlioMessage - exception occurred when processing"
                    " entity ID %s: %s" % (parsed_msg.data.entity_id, e),
                    exc_info=True,
                )
                to_yield = False

        if to_yield:
            out_proto_msg = postprocess_klio_message(self, parsed_msg)
            yield out_proto_msg.SerializeToString()
        else:
            # Supposedly we should explicitly return nothing:
            # https://stackoverflow.com/a/50537659/1579977
            return

    return wrapper

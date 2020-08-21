# Copyright 2020 Spotify AB

import functools
import types

from klio_core.proto import klio_pb2

from klio.message_handler import common
from klio.message_handler import exceptions


# [batch dev] attemping to make this a little generic so it can (eventually)
# be used with transforms other than DoFns
def _to_klio_message(incoming_message, kconfig=None, logger=None):
    # TODO: when making a generic de/ser func, be sure to assert
    # kconfig and logger exists
    parsed_message = klio_pb2.KlioMessage()

    try:
        parsed_message.ParseFromString(incoming_message)

    except klio_pb2._message.DecodeError as e:
        if kconfig.job_config.allow_non_klio_messages:
            # We are assuming that we have been given "raw" data that is not in
            # the form of a serialized KlioMessage.
            parsed_message.data.element = incoming_message
        else:
            logger.error(
                "Can not parse incoming message. To support non-Klio "
                "messages, add `job_config.allow_non_klio_messages = true` "
                "in the job's `klio-job.yaml` file."
            )
            raise e

    if (
        parsed_message.version
        and parsed_message.version is not klio_pb2.Version.V2
    ):
        msg = (
            "Job is not configured to parse version '%s' of KlioMessage.\n"
            "Errored message: %s" % (parsed_message.version, parsed_message)
        )
        raise exceptions.KlioVersionMismatch(msg)

    return parsed_message


# TODO: maybe figure out a wa to include a SerializeToString call
def _from_klio_message(klio_message, payload=None):
    if payload:
        # if the user just returned exactly what they received in the
        # process method; let's avoid recursive payloads
        if payload == klio_message.data:
            payload = b""

    if not payload:
        # be sure to clear out old payload if there's no new payload
        payload = b""

    else:
        if not isinstance(payload, bytes):
            try:
                payload = bytes(payload, "utf-8")
            except TypeError:
                msg = (
                    "Returned payload could not be coerced to `bytes`.\n"
                    "Erroring payload: {}\nErroring KlioMessage: {}".format(
                        payload, klio_message
                    )
                )
                raise exceptions.KlioMessagePayloadException(msg)

    # [batch dev] TODO: figure out how/where to clear out this payload
    # when publishing to pubsub (and potentially other output transforms)
    klio_message.data.payload = payload

    return klio_message


def postprocess_klio_message(dofn_inst, parsed_message, payload=None):
    parsed_message = _from_klio_message(parsed_message, payload)

    klio_job = klio_pb2.KlioJob()
    klio_job.ParseFromString(dofn_inst._klio.job)
    parsed_message.metadata.visited.extend([klio_job])

    if parsed_message.metadata.ping:
        visited = parsed_message.metadata.visited
        traversed_dag = " -> ".join(
            "%s::%s" % (str(j.gcp_project), str(j.job_name)) for j in visited
        )
        traversed_dag = "%s (current job)" % traversed_dag

        base_log_msg = "KlioMessage received in 'ping' mode"
        log_msg = "%s - Element ID: %s - Path: %s" % (
            base_log_msg,
            parsed_message.data.element,
            traversed_dag,
        )
        dofn_inst._klio.logger.info(log_msg)

    return parsed_message


def check_input_data_exists(dofn_inst, parsed_message, current_job):
    # [batch dev] TODO: include transform name in the log message
    # [batch dev] TODO: standardize log prefixes (i.e. dropping klio msg)
    #             so that it's easier to filter/make metrics
    # [batch dev] QUESTION/TODO: should this data existence checks pass in all
    #             of data or not? probably all of data (incl payloads)
    input_exists = dofn_inst.input_data_exists(parsed_message.data.element)
    if not input_exists:
        dofn_inst._klio.logger.info(
            "Dropping KlioMessage - Input data for does not yet exist for "
            " element '%s'." % parsed_message.data.element
        )
        # [batch dev] unlike v1, we're not triggering any parents. but
        # we will probably have to add a conditional (if streaming, trigger,
        # if batch, not) - and maybe even allow the user to configure
        # trigger-ability per transform - maybe to allow only triggering on
        #  the first transform after event IO?
        return common.MessageState.DROP

    return common.MessageState.PROCESS


def check_output_data_exists(dofn_inst, parsed_message):
    if parsed_message.metadata.force is True:
        return False

    # [batch dev] TODO: include transform name in the log message for better
    #             differentiation when using multiple transforms
    # [batch dev] TODO: standardize log prefixes (i.e. dropping klio msg)
    #             so that it's easier to filter/make metrics
    # [batch dev] QUESTION/TODO: should this data existence checks pass in all
    #             of data or not? probably all of data (incl payloads)
    output_exists = dofn_inst.output_data_exists(parsed_message.data.element)
    if output_exists:
        dofn_inst._klio.logger.debug(
            "Skipping element %s: output data already exists."
            % parsed_message.data.element
        )
        return True

    return False


def preprocess_klio_message(dofn_inst, incoming_message):
    message_state = common.MessageState.PROCESS

    parsed_message = _to_klio_message(
        incoming_message,
        kconfig=dofn_inst._klio.config,
        logger=dofn_inst._klio.logger,
    )

    current_job = klio_pb2.KlioJob()
    current_job.ParseFromString(dofn_inst._klio.job)

    # [batch dev] unlike v1, we're skipping checks if this job is
    # downstream or not (aka not having top-down/bottom-up execution)
    # just for now

    parsed_message = common.update_audit_log(
        dofn_inst, parsed_message, current_job
    )

    if parsed_message.metadata.ping:
        return parsed_message, common.MessageState.PASS_THRU

    should_skip = check_output_data_exists(dofn_inst, parsed_message)
    if should_skip is True:
        return parsed_message, common.MessageState.PASS_THRU

    message_state = check_input_data_exists(
        dofn_inst, parsed_message, current_job
    )

    return parsed_message, message_state


def parse_klio_message(process_method):
    @functools.wraps(process_method)
    def wrapper(self, incoming_item, *args, **kwargs):
        to_yield = True
        ret = None

        try:
            parsed_msg, msg_state = preprocess_klio_message(
                self, incoming_item
            )
        except Exception as e:
            self._klio.logger.error(
                "Dropping KlioMessage - exception occurred when parsing "
                "item %s\nError: %s" % (incoming_item, e),
                exc_info=True,
            )
            msg_state = common.MessageState.DROP

        if msg_state is common.MessageState.DROP:
            to_yield = False

        if msg_state is common.MessageState.PROCESS:
            try:
                # [batch dev] unlike v1, we're ignoring timeouts and retries
                # just for now (TODO - add back in!)
                ret = process_method(self, parsed_msg.data)
                # if DoFn.process method `yield`s instead of `return`s
                if isinstance(ret, types.GeneratorType):
                    ret = next(ret)

            except Exception as e:
                self._klio.logger.error(
                    "Dropping KlioMessage - exception occurred when "
                    "processing message %s\nError: %s" % (parsed_msg, e),
                    exc_info=True,
                )
                to_yield = False

        if to_yield:
            try:
                out_proto_msg = postprocess_klio_message(
                    self, parsed_msg, payload=ret
                )
                yield out_proto_msg.SerializeToString()
            except Exception as e:
                self._klio.logger.error(
                    "Dropping KlioMessage - exception occurred when "
                    "postprocessing message %s\nError: %s" % (parsed_msg, e),
                    exc_info=True,
                )
                return
        else:
            # Supposedly we should explicitly return nothing:
            # https://stackoverflow.com/a/50537659/1579977
            return

    return wrapper

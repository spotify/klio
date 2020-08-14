# Copyright 2020 Spotify AB

from apache_beam import pvalue

from klio_core.proto import klio_pb2

from klio.message import exceptions


# [batch dev] attemping to make this a little generic so it can (eventually)
# be used with transforms other than DoFns
def to_klio_message(incoming_message, kconfig=None, logger=None):
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

    return parsed_message


def from_klio_message(klio_message, payload=None):
    tagged, tag = False, None
    if isinstance(payload, pvalue.TaggedOutput):
        tagged = True
        tag = payload.tag
        payload = payload.value

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

    if tagged:
        return pvalue.TaggedOutput(tag, klio_message.SerializeToString())

    return klio_message.SerializeToString()

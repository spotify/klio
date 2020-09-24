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

from apache_beam import pvalue

from klio_core.proto import klio_pb2

from klio.message import exceptions


def _handle_msg_compat(parsed_message):
    if parsed_message.version is klio_pb2.Version.V1:
        if parsed_message.data.entity_id and not parsed_message.data.element:
            # make v1 messages compatible with v2
            parsed_message.data.element = bytes(
                parsed_message.data.entity_id, "utf-8"
            )
        return parsed_message

    if parsed_message.version is klio_pb2.Version.V2:
        # is it safe to assume if a message is already labeled as v2, it should
        # have an element or payload? i.e. not just entity_id?
        return parsed_message

    if parsed_message.data.entity_id and not parsed_message.data.element:
        # assume v1 message
        parsed_message.version = klio_pb2.Version.V1
        # make v1 messages compatible with v2
        parsed_message.data.element = bytes(
            parsed_message.data.entity_id, "utf-8"
        )

    elif not parsed_message.data.entity_id and not parsed_message.data.element:
        # assume v1 message
        parsed_message.version = klio_pb2.Version.V1

    elif parsed_message.data.element and not parsed_message.data.entity_id:
        # assume v2 message
        parsed_message.version = klio_pb2.Version.V2

    return parsed_message


# [batch dev] attemping to make this a little generic so it can (eventually)
# be used with transforms other than DoFns
def to_klio_message(incoming_message, kconfig=None, logger=None):
    """Serialize ``bytes`` to a :ref:`KlioMessage <klio-message>`.

    .. tip::

        Set ``job_config.allow_non_klio_messages`` to ``True`` in
        ``klio-job.yaml`` in order to process non-``KlioMessages`` as
        regular ``bytes``. This function will create a new ``KlioMessage``
        and set the incoming ``bytes`` to ``KlioMessage.data.element``.

    Args:
        incoming_message (bytes): Incoming bytes to parse into a \
            ``KlioMessage``.
        kconfig (klio_core.config.KlioConfig): the current job's
            configuration.
        logger (logging.Logger): the logger associated with the Klio
            job.
    Returns:
        klio_core.proto.klio_pb2.KlioMessage: a ``KlioMessage``.
    Raises:
        klio_core.proto.klio_pb2._message.DecodeError: incoming message
            can not be parsed into a ``KlioMessage`` and
            ``job_config.allow_non_klio_messages`` in ``klio-job.yaml``
            is set to ``False``.
    """
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

    parsed_message = _handle_msg_compat(parsed_message)
    return parsed_message


def _handle_v2_payload(klio_message, payload):
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
    return payload


def from_klio_message(klio_message, payload=None):
    """Deserialize a given :ref:`KlioMessage <klio-message>` to ``bytes``.

    Args:
        klio_message (klio_core.proto.klio_pb2.KlioMessage): the
            ``KlioMessage`` in which to deserialize into ``bytes``
        payload (bytes or str): Optional ``bytes`` or ``str`` to update
            the value of ``KlioMessage.data.payload`` with before
            deserializing into bytes. Default: ``None``.
    Returns:
        bytes: a ``KlioMessage`` as ``bytes``.
    Raises:
        exceptions.KlioMessagePayloadException: the provided payload
            value cannot be coerced into ``bytes``.
    """
    tagged, tag = False, None
    if isinstance(payload, pvalue.TaggedOutput):
        tagged = True
        tag = payload.tag
        payload = payload.value

    # only update payload if it's a v2 message.
    if klio_message.version == klio_pb2.Version.V2:
        payload = _handle_v2_payload(klio_message, payload)
        # [batch dev] TODO: figure out how/where to clear out this payload
        # when publishing to pubsub (and potentially other output transforms)
        klio_message.data.payload = payload

    if tagged:
        return pvalue.TaggedOutput(tag, klio_message.SerializeToString())

    return klio_message.SerializeToString()

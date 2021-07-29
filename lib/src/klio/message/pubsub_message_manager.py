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

import logging
import threading
import time

from concurrent import futures

from google.cloud import pubsub as g_pubsub

from klio_core.proto import klio_pb2


ENTITY_ID_TO_ACK_ID = {}
MESSAGE_LOCK = threading.Lock()
_CACHED_THREADPOOL_EXEC = None


def _get_or_create_executor():
    # We create one threadpool executor since the MessageManager does
    # get initialized more than once (pretty frequently, actually). So
    # let's reuse our executor rather than re-create it.
    global _CACHED_THREADPOOL_EXEC
    if _CACHED_THREADPOOL_EXEC is None:
        # max_workers is equal to the number of threads we want to run in
        # the background - 1 message maanger, and 1 heartbeat
        _CACHED_THREADPOOL_EXEC = futures.ThreadPoolExecutor(
            thread_name_prefix="KlioMessageManager", max_workers=2
        )

    return _CACHED_THREADPOOL_EXEC


class PubSubKlioMessage:
    """Contains state needed to manage ACKs for a KlioMessage"""

    def __init__(self, ack_id, kmsg_id):
        self.ack_id = ack_id
        self.kmsg_id = kmsg_id
        self.last_extended = None
        self.ext_duration = None
        self.event = threading.Event()

    def extend(self, duration):
        self.last_extended = time.monotonic()
        self.ext_duration = duration

    def __repr__(self):
        return f"PubSubKlioMessage(kmsg_id={self.kmsg_id})"


class MessageManager:
    """Manages the ack deadline for in-progress KlioMessages.

    Extends the ack deadline while the KlioMessage is still processing, and
    stops extending them when the message is done.

    This class is used by ``KlioPubSubReadEvaluator`` to manage message
    acknowledgement.

    Warning: non-KlioMessages are not (yet) supported. ``klio-job.yaml::
    job_config.allow_non_klio_messages`` must be ``False``.

    Usage:

    .. code-block:: python
        m = MessageManager("subscription-name")
        m.start_threads()
    """

    DEFAULT_DEADLINE_EXTENSION = 30

    def __init__(self, sub_name, heartbeat_sleep=10, manager_sleep=3):
        """Initialize a MessageManager instance.

        Args:
             sub_name(str): PubSub subscription name to listen on.
             heartbeat_sleep(float):
                Seconds to sleep between heartbeat messages.
             manager_sleep(float):
                Seconds to sleep between deadline extension checks.
        """
        self._client = g_pubsub.SubscriberClient()
        self._sub_name = sub_name
        self.heartbeat_sleep = heartbeat_sleep
        self.manager_sleep = manager_sleep
        self.messages = []
        self.mgr_logger = logging.getLogger(
            "klio.gke_direct_runner.message_manager"
        )
        self.hrt_logger = logging.getLogger("klio.gke_direct_runner.heartbeat")
        self.executor = _get_or_create_executor()

    def manage(self, message):
        """Continuously track in-progress messages and extends their deadlines.

        Args:
            to_sleep(float): Seconds to sleep between checks.
        """
        while True:
            with MESSAGE_LOCK:
                msg_is_active = message.kmsg_id in ENTITY_ID_TO_ACK_ID
            if msg_is_active:
                self._maybe_extend(message)
                time.sleep(self.manager_sleep)
            else:
                self.remove(message)
                break

    def heartbeat(self, message):
        """Continuously log heartbeats for in-progress messages.

        Args:
            to_sleep(float): Second to sleep between log messages.
        """
        while True:
            with MESSAGE_LOCK:
                msg_is_active = message.kmsg_id in ENTITY_ID_TO_ACK_ID
            if msg_is_active:
                self.hrt_logger.info(
                    f"Job is still processing {message.kmsg_id}..."
                )
                time.sleep(self.heartbeat_sleep)
            else:
                self.hrt_logger.debug(
                    f"Job is no longer processing {message.kmsg_id}."
                )
                break

    def _maybe_extend(self, message):
        """Check to see if message is done and extends deadline if not.

        Deadline extension is only done when 80% of the message's extension
        duration has passed.

        Args:
            message(PubSubKlioMessage): In-progress message to check.
        """
        diff = 0
        now = time.monotonic()
        if message.last_extended is not None:
            diff = now - message.last_extended

        # taking 80% of the deadline extension as a
        # threshold to comfortably request a message deadline
        # extension before the deadline comes around
        threshold = message.ext_duration * 0.8
        if message.last_extended is None or diff >= threshold:
            self.extend_deadline(message)
        else:
            self.mgr_logger.debug(
                f"Skipping extending Pub/Sub ack deadline for {message}"
            )

    def extend_deadline(self, message, duration=None):
        """Extend deadline for a PubSubKlioMessage.

        Args:
            message(PubSubKlioMessage): The message to extend the deadline for.
            duration(float): Seconds. If not specified, defaults to
                MessageManager.DEFAULT_DEADLINE_EXTENSION.
        """
        if duration is None:
            duration = self.DEFAULT_DEADLINE_EXTENSION
        request = {
            "subscription": self._sub_name,
            "ack_ids": [message.ack_id],
            "ack_deadline_seconds": duration,  # seconds
        }
        try:
            # TODO: this method also has `retry` and `timeout` kwargs which
            # we may be interested in using
            self._client.modify_ack_deadline(**request)
        except Exception as e:
            self.mgr_logger.error(
                f"Error encountered when trying to extend deadline for "
                f"{message} with ack ID '{message.ack_id}': {e}",
                exc_info=True,
            )
            self.mgr_logger.warning(
                f"The message {message} may be re-delivered due to Klio's "
                "inability to extend its deadline."
            )
        else:
            self.mgr_logger.debug(
                f"Extended Pub/Sub ack deadline for {message} by {duration}s"
            )
        message.extend(duration)

    @staticmethod
    def _convert_raw_pubsub_message(ack_id, pmessage):
        # TODO: either use klio.message.serializer.to_klio_message, or
        # figure out how to handle when a parsed_message can't be parsed
        # into a KlioMessage (will need to somehow get the klio context)
        kmsg = klio_pb2.KlioMessage()
        kmsg.ParseFromString(pmessage.data)
        entity_id = kmsg.data.element.decode("utf-8")
        psk_msg = PubSubKlioMessage(ack_id, entity_id)
        return psk_msg

    def add(self, ack_id, raw_pubsub_message):
        """Add message to set of in-progress messages.

        Messages added via this method will have their deadlines extended
        until they are finished processing.

        Args:
            ack_id (str): Pub/Sub message's ack ID
            raw_pubsub_message (apache_beam.io.gcp.pubsub.PubsubMessage):
                Pub/Sub message to add.
        """
        psk_msg = self._convert_raw_pubsub_message(ack_id, raw_pubsub_message)

        self.mgr_logger.debug(f"Received {psk_msg.kmsg_id} from Pub/Sub.")
        self.extend_deadline(psk_msg)
        ENTITY_ID_TO_ACK_ID[psk_msg.kmsg_id] = psk_msg
        self.executor.submit(self.manage, psk_msg)
        self.executor.submit(self.heartbeat, psk_msg)

    def remove(self, psk_msg):
        """Remove message from set of in-progress messages.

        Messages removed via this method will be acknowledged.

        Args:
            psk_msg (PubSubKlioMessage): Message to remove.
        """
        try:
            # TODO: this method also has `retry`, `timeout` and metadata
            # kwargs which we may be interested in using
            self._client.acknowledge(self._sub_name, [psk_msg.ack_id])
        except Exception as e:
            # Note: we are just catching & logging any potential error we
            # encounter. We will still remove the message from our message
            # manager so we no longer try to extend.
            self.mgr_logger.error(
                f"Error encountered when trying to acknowledge {psk_msg} with "
                f"ack ID '{psk_msg.ack_id}': {e}",
                exc_info=True,
            )
            self.mgr_logger.warning(
                f"The message {psk_msg} may be re-delivered due to Klio's "
                "inability to acknowledge it."
            )
        else:
            self.mgr_logger.info(
                f"Acknowledged {psk_msg.kmsg_id}. Job is no longer processing "
                "this message."
            )

    @staticmethod
    def mark_done(kmsg_or_bytes):
        """Mark a KlioMessage as done and to be removed from handling.

        This method just sets the PubSubKlioMessage.event object where then
        in the next iteration in `MessageManager.manage`, it is then
        acknowledged and removed from further "babysitting".

        Args:
            kmsg_or_bytes (klio_pb2.KlioMessage or bytes): the KlioMessage
                (or a KlioMessage that has been serialzied to bytes) to be
                marked as done.
        """
        kmsg = kmsg_or_bytes
        mm_logger = logging.getLogger("klio.gke_direct_runner.message_manager")

        # Wrap in a general try/except to make sure this method returns cleanly,
        # aka no raised errors that may prevent the pipeline from consuming
        # the next message available. Not sure if this causes problems
        # of being unable to pull a message, but at least it's for
        # sanity.
        try:
            # TODO: either use klio.message.serializer.to_klio_message, or
            # figure out how to handle when a parsed_message can't be parsed
            # into a KlioMessage (will need to somehow get the klio context).
            if not isinstance(kmsg_or_bytes, klio_pb2.KlioMessage):
                kmsg = klio_pb2.KlioMessage()
                kmsg.ParseFromString(kmsg_or_bytes)

            entity_id = kmsg.data.element.decode("utf-8")

            # This call to remove the message from the dict ENTITY_ID_TO_ACK_ID
            # will tell the MessageManager that this message is now ready to
            # be acknowledged and no longer being worked upon.
            with MESSAGE_LOCK:
                msg = ENTITY_ID_TO_ACK_ID.pop(entity_id, None)

            if not msg:
                # NOTE: this logger exists as `self.mgr_logger`, but this method
                # needs to be a staticmethod so we don't need to unnecessarily
                # init the class in order to just mark a message as done.
                mm_logger.warn(
                    f"Unable to acknowledge {entity_id}: Not found."
                )
        except Exception as e:
            # Catch all Exceptions so that the pipeline doesn't enter into
            # a weird state because of an uncaught error.
            mm_logger.warning(
                f"Error occurred while trying to remove message {kmsg}: {e}",
                exc_info=True,
            )

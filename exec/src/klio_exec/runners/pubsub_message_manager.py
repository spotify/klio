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

import apache_beam as beam

from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.direct import transform_evaluator
from apache_beam.utils import timestamp as beam_timestamp
from google.api_core import exceptions as g_exceptions
from google.cloud import pubsub as g_pubsub

from klio_core.proto import klio_pb2


ENTITY_ID_TO_ACK_ID = {}


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

    Usage:

    .. code-block:: python
        m = MessageManager("subscription-name")
        m.start_threads()
    """

    DEFAULT_DEADLINE_EXTENSION = 30

    def __init__(self, sub_name, heartbeat_sleep=10, manager_sleep=5):
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
        self.messages_lock = threading.Lock()
        self.mgr_logger = logging.getLogger(
            "klio.gke_direct_runner.message_manager"
        )
        self.hrt_logger = logging.getLogger("klio.gke_direct_runner.heartbeat")

    def start_threads(self):
        """Launch message management threads.

        Two threads are launched:
            1. The manager thread which keeps track of in-progress messages
            and extends their deadlines.
            2. The heartbeat thread which logs which messages
            are still in-progress
        """
        mgr_thread = threading.Thread(
            target=self.manage,
            args=(self.manager_sleep,),
            name="KlioMessageManager",
            daemon=True,
        )
        mgr_thread.start()

        heartbeat_thread = threading.Thread(
            target=self.heartbeat,
            args=(self.heartbeat_sleep,),
            name="KlioMessageHeartbeat",
            daemon=True,
        )
        heartbeat_thread.start()

    def manage(self, to_sleep):
        """Continuously track in-progress messages and extends their deadlines.

        Args:
            to_sleep(float): Seconds to sleep between checks.
        """
        while True:
            to_remove = []
            with self.messages_lock:
                for message in self.messages:
                    should_remove = self._extend_or_remove(message)
                    if should_remove:
                        to_remove.append(message)

            for message in to_remove:
                self.remove(message)

            time.sleep(to_sleep)

    def heartbeat(self, to_sleep):
        """Continuously log heartbeats for in-progress messages.

        Args:
            to_sleep(float): Second to sleep between log messages.
        """
        while True:
            for message in self.messages:
                self.hrt_logger.info(
                    f"Job is still processing {message.kmsg_id}..."
                )
            time.sleep(to_sleep)

    def _extend_or_remove(self, message):
        """Check to see if message is done and extends deadline if not.

        Deadline extension is only done when 80% of the message's extension
        duration has passed.

        Args:
            message(PubSubKlioMessage): In-progress message to check.
        Returns:
            True if work associated with message is complete; False otherwise.
        """
        diff = 0
        if not message.event.is_set():
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
            return False

        return True

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

    def add(self, message):
        """Add message to set of in-progress messages.

        Messages added via this method will have their deadlines extended
        until they are finished processing.

        Args:
            message(PubSubKlioMessage): Message to add.
        """
        self.mgr_logger.debug(f"Received {message.kmsg_id} from Pub/Sub.")
        self.extend_deadline(message)
        ENTITY_ID_TO_ACK_ID[message.kmsg_id] = message
        with self.messages_lock:
            self.messages.append(message)

    def remove(self, message):
        """Remove message from set of in-progress messages.

        Messages removed via this method will be acknowledged.

        Args:
            message(PubSubKlioMessage): Message to remove.
        """
        try:
            # TODO: this method also has `retry`, `timeout` and metadata
            # kwargs which we may be interested in using
            self._client.acknowledge(self._sub_name, [message.ack_id])
        except Exception as e:
            # Note: we are just catching & logging any potential error we
            # encounter. We will still remove the message from our message
            # manager so we no longer try to extend.
            self.mgr_logger.error(
                f"Error encountered when trying to acknowledge {message} with "
                f"ack ID '{message.ack_id}': {e}",
                exc_info=True,
            )
            self.mgr_logger.warning(
                f"The message {message} may be re-delivered due to Klio's "
                "inability to acknowledge it."
            )
        else:
            self.mgr_logger.info(
                f"Acknowledged {message.kmsg_id}. Job is no longer processing "
                "this message."
            )
        with self.messages_lock:
            index = self.messages.index(message)
            self.messages.pop(index)
        ENTITY_ID_TO_ACK_ID.pop(message.kmsg_id, None)


class KlioPubSubReadEvaluator(transform_evaluator._PubSubReadEvaluator):
    """PubSubReadEvaluator for Klio's GkeDirectRunner.

    Behaves in the same way as _PubSubReadEvaluator, except for the fact
    that it acknowledges PubSub messages after they are done processing.
    """

    def __init__(self, *args, **kwargs):
        super(KlioPubSubReadEvaluator, self).__init__(*args, **kwargs)
        # Heads up: self._sub_name is from init'ing parent class
        self.sub_client = g_pubsub.SubscriberClient()
        self.message_manager = MessageManager(self._sub_name)
        self.message_manager.start_threads()
        self.logger = logging.getLogger("klio.pubsub_read_evaluator")

    def _read_from_pubsub(self, timestamp_attribute):
        # Klio maintainer note: This code is the eact same logic in
        # _PubSubReadEvaluator._read_from_pubsub with the
        # following changes:
        # 1. Import statements that were originally inside this method
        #    was moved to the top of this module.
        # 2. Import statements adjusted to import module and not objects
        #    according to the google style guide.
        # 3. The functionalty we needed to override, which skips auto-acking
        #    consumed pubsub messages, and adds them to the MessageManager
        #    to handle deadline extension and acking once done.

        def _get_element(ack_id, message):
            parsed_message = beam_pubsub.PubsubMessage._from_message(message)
            if (
                timestamp_attribute
                and timestamp_attribute in parsed_message.attributes
            ):
                rfc3339_or_milli = parsed_message.attributes[
                    timestamp_attribute
                ]
                try:
                    timestamp = beam_timestamp.Timestamp(
                        micros=int(rfc3339_or_milli) * 1000
                    )
                except ValueError:
                    try:
                        timestamp = beam_timestamp.Timestamp.from_rfc3339(
                            rfc3339_or_milli
                        )
                    except ValueError as e:
                        raise ValueError("Bad timestamp value: %s" % e)
            else:
                timestamp = beam_timestamp.Timestamp(
                    message.publish_time.seconds,
                    message.publish_time.nanos // 1000,
                )

            # TODO: either use klio.mssage.serializer.to_klio_message, or
            # figure out how to handle when a parsed_message can't be parsed
            # into a KlioMessage
            kmsg = klio_pb2.KlioMessage()
            kmsg.ParseFromString(parsed_message.data)
            entity_id = kmsg.data.element.decode("utf-8")

            pmsg = PubSubKlioMessage(ack_id, entity_id)
            self.message_manager.add(pmsg)

            return timestamp, parsed_message

        try:
            response = self.sub_client.pull(
                self._sub_name, max_messages=1, return_immediately=True
            )
            results = [
                _get_element(rm.ack_id, rm.message)
                for rm in response.received_messages
            ]

        # only catching/ignoring this for now - if new exceptions raise, we'll
        # figure it out as they come on how to handle them
        except g_exceptions.DeadlineExceeded as e:
            # this seems mostly a benign error when there are 20+ seconds
            # between messages
            self.logger.debug(e)

        finally:
            self.sub_client.api.transport.channel.close()

        return results


class KlioTransformEvaluatorRegistry(
    transform_evaluator.TransformEvaluatorRegistry
):
    """TransformEvaluatorRegistry for Klio

    Makes DirectRunner's ReadFromPubSub use KlioPubSubReadEvaluator.
    """

    def __init__(self, *args, **kwargs):
        super(KlioTransformEvaluatorRegistry, self).__init__(*args, **kwargs)
        self._evaluators[
            direct_runner._DirectReadFromPubSub
        ] = KlioPubSubReadEvaluator


class KlioAckInputMessage(beam.DoFn):
    """Message acknowledgement DoFn.

    Marks a KlioMessage as done after it is finished processing.

    Used in conjunction with the KlioPubSubReadEvaluator, which tracks
    KlioMessages and extends their deadlines until they are marked as done.
    """

    def process(self, element):
        kmsg = klio_pb2.KlioMessage()
        kmsg.ParseFromString(element)
        entity_id = kmsg.data.element.decode("utf-8")

        msg = ENTITY_ID_TO_ACK_ID.get(entity_id)
        # This call, `set`, will tell the MessageManager that this
        # message is now ready to be acknowledged and no longer being
        # worked upon.
        if msg:
            msg.event.set()
        else:
            mm_logger = logging.getLogger(
                "klio.gke_direct_runner.message_manager"
            )
            mm_logger.warn(f"Unable to acknowledge {entity_id}: Not found.")

        yield element

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

from apache_beam.io.gcp import pubsub as beam_pubsub
from apache_beam.runners.direct import direct_runner
from apache_beam.runners.direct import transform_evaluator
from apache_beam.utils import timestamp as beam_timestamp
from google.api_core import exceptions as g_exceptions
from google.cloud import pubsub as g_pubsub

from klio.message import pubsub_message_manager as pmsg_mgr


class KlioPubSubReadEvaluator(transform_evaluator._PubSubReadEvaluator):
    """PubSubReadEvaluator for Klio's GkeDirectRunner.

    Behaves in the same way as _PubSubReadEvaluator, except for the fact
    that it acknowledges PubSub messages after they are done processing.
    """

    def __init__(self, *args, **kwargs):
        super(KlioPubSubReadEvaluator, self).__init__(*args, **kwargs)
        # Heads up: self._sub_name is from init'ing parent class
        self.sub_client = g_pubsub.SubscriberClient()
        self.message_manager = pmsg_mgr.MessageManager(self._sub_name)
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

            self.message_manager.add(ack_id, parsed_message)

            return timestamp, parsed_message

        results = None
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

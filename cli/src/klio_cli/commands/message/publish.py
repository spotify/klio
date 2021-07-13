# Copyright 2019-2020 Spotify AB
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

import functools
import logging

import emoji

from google.api_core import exceptions as gapi_exceptions
from google.cloud import pubsub

from klio_core.proto.v1beta1 import klio_pb2


def _create_publisher(topic):
    client = pubsub.PublisherClient()

    try:
        client.get_topic(request={"topic": topic})
    except gapi_exceptions.NotFound:
        msg = (
            ":persevere: Topic '{}' not found. Is there a running job "
            "subscribed to this topic? or is there a typo in the configured "
            "topic?".format(topic)
        )
        logging.error(emoji.emojize(msg, use_aliases=True))
        raise SystemExit(1)
    except Exception:
        raise

    return functools.partial(client.publish, topic=topic)


def _get_current_klio_job(config):
    klio_job = klio_pb2.KlioJob()
    klio_job.job_name = config.job_name
    klio_job.gcp_project = config.pipeline_options.project
    return klio_job


# [batch dev] TODO: rename entity_id variable in this module
def _create_pubsub_message(entity_id, job, force, ping, top_down, msg_version):
    kmsg = klio_pb2.KlioMessage()
    kmsg.version = msg_version

    if msg_version == 1:
        kmsg.data.entity_id = entity_id
        if not top_down:
            kmsg.metadata.downstream.extend([job])
    elif msg_version == 2:
        kmsg.data.element = bytes(entity_id, "utf-8")
        if not top_down:
            kmsg.metadata.intended_recipients.limited.recipients.extend([job])
        else:
            kmsg.metadata.intended_recipients.anyone.SetInParent()

    kmsg.metadata.ping = ping
    kmsg.metadata.force = force
    return kmsg.SerializeToString()


def _publish_messages(
    config, entity_ids, ping, force, top_down, allow_non_klio, msg_version
):
    current_job = _get_current_klio_job(config)
    publish = _create_publisher(config.job_config.events.inputs[0].topic)

    success_ids = []
    fail_ids = []
    for entity_id in entity_ids:
        if not allow_non_klio:
            message = _create_pubsub_message(
                entity_id, current_job, ping, force, top_down, msg_version
            )
        else:
            # TODO: should rename argument to something more abstract (@lynn)
            message = bytes(entity_id.encode("utf-8"))

        try:
            future = publish(data=message)
            # block until message is published or exception is raised
            future.result()
            success_ids.append(entity_id)

        except Exception as e:
            msg = "Failed to publish message for entity '%s': %s" % (
                entity_id,
                e,
            )
            logging.warning(msg)
            fail_ids.append(entity_id)

    return success_ids, fail_ids


def publish_messages(
    config,
    entity_ids,
    force=False,
    ping=False,
    top_down=False,
    allow_non_klio=False,
    msg_version=None,
):
    # [batch dev] TODO: if we use KlioConfig, we don't have to find the
    #             version
    # [batch dev] maintaining backwards compatibility if this code path
    # is executed by some other way other than via cli.py::publish.
    if msg_version is None:
        msg_version = config.version

    if not config.job_config.events.inputs:
        msg = "No input topics configured for {} :-1:".format(config.job_name)
        logging.error(emoji.emojize(msg, use_aliases=True))
        raise SystemExit(1)

    logging.info(
        "Publishing {} messages to {}'s input topic {}".format(
            len(entity_ids),
            config.job_name,
            # [batch dev] should we support multiple inputs in the future?
            config.job_config.events.inputs[0].topic,
        )
    )
    success, fail = _publish_messages(
        config, entity_ids, force, ping, top_down, allow_non_klio, msg_version
    )

    if success:
        msg = ":boom: Successfully published {} messages.".format(len(success))
        logging.info(emoji.emojize(msg, use_aliases=True))
    if fail:
        msg = (
            ":persevere: Failed to publish the following {} entity "
            "IDs: {}".format(len(fail), ", ".join(fail))
        )
        logging.warning(emoji.emojize(msg, use_aliases=True))

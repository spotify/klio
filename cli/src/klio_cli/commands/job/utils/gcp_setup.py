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
import logging

import google.api_core.exceptions as gcp_exceptions
from google.cloud import pubsub_v1
from google.cloud import storage

from klio_cli.utils import stackdriver_utils as sd_utils


def _create_stackdriver_group_if_missing(project, job_name, region):
    dashboard_url = None
    # first see if dashboard is already created for job; otherwise it will
    # create a new one of the same name but a new ID
    try:
        dashboard_url = sd_utils.get_stackdriver_group_url(
            project, job_name, region
        )
    except Exception as e:
        msg = (
            "Error encountered while trying to determine whether a "
            "Stackdriver dashboard already exists for this job: {}. "
            "Skipping...".format(e)
        )
        logging.warning(msg)
        return

    if dashboard_url:
        msg = (
            "Reusing existing dashboard previously created for "
            "'{}': {}.".format(job_name, dashboard_url)
        )
        logging.info(msg)
    else:
        # nothing already exists; we'll create one now
        dashboard_url = sd_utils.create_stackdriver_group(
            project, job_name, region
        )

    return dashboard_url


def _create_topic_if_missing(topic_path, publisher_client, project):
    try:
        publisher_client.create_topic(topic_path)
        msg = "Created topic {} in project {}".format(topic_path, project)
        logging.info(msg)
    # Topic already exists.
    except gcp_exceptions.AlreadyExists:
        msg = "Topic {} already exists in project {}".format(
            topic_path, project
        )
        logging.info(msg)
    # InvalidArgument: Invalid topic name.
    # PermissionDenied: Lacking permissions to create topics in project.
    # NotFound: Requested project not found or user does not have access to it.
    except Exception as e:
        logging.error("Could not create topic: {}".format(topic_path))
        logging.error(e)
        raise SystemExit(1)


def _create_subscription_if_missing(subscriber_client, topic, subscription):

    try:
        subscriber_client.create_subscription(name=subscription, topic=topic)
        msg = "Created subscription {} to topic {}".format(subscription, topic)
        logging.info(msg)
    except gcp_exceptions.AlreadyExists:
        msg = "Subscriptions '{}' already exists for topic '{}'".format(
            subscription, topic
        )
        logging.info(msg)
    except Exception as e:
        logging.error(
            "Could not create subscription '{}' to topic '{}'".format(
                subscription, topic
            )
        )
        logging.error(e)
        raise SystemExit(1)


def _create_bucket_if_missing(bucket_name, storage_client, project):
    try:
        storage_client.create_bucket(bucket_name)
        msg = "Created bucket {} in project {}".format(bucket_name, project)
        logging.info(msg)
    # Bucket already exists.
    except gcp_exceptions.Conflict:
        msg = "Bucket {} already exists in project {}".format(
            bucket_name, project
        )
        logging.info(msg)
    # Forbidden: No storage.buckets.create access to project.
    # BadRequest: Unknown project id.
    except Exception as e:
        logging.error("Could not create bucket: gs://{}/".format(bucket_name))
        logging.error(e)
        raise SystemExit(1)


def create_topics(context):
    inputs = context["job_options"]["inputs"]
    outputs = context["job_options"]["outputs"]
    project = context["pipeline_options"]["project"]
    job_name = context["job_name"]

    logging.info(
        "Creating input topics & subscriptions for job {}".format(job_name)
    )
    publisher_client = pubsub_v1.PublisherClient()
    subscriber_client = pubsub_v1.SubscriberClient()
    for input_ in inputs:
        input_topic = input_["topic"]
        subscription = input_["subscription"]
        _create_topic_if_missing(input_topic, publisher_client, project)
        _create_subscription_if_missing(
            subscriber_client, input_topic, subscription
        )

    logging.info(
        "Creating output topics & locations for job {}".format(job_name)
    )

    for output in outputs:
        output_topic = output["topic"]
        _create_topic_if_missing(output_topic, publisher_client, project)


def create_buckets(context):
    outputs = context["job_options"]["outputs"]
    project = context["pipeline_options"]["project"]
    job_name = context["job_name"]

    logging.info("Creating output locations for job {}".format(job_name))
    storage_client = storage.Client(project=project)

    for output in outputs:
        location = output["data_location"]
        if not location.startswith("gs://"):
            msg = "Unsupported location type, skipping: {}".format(location)
            logging.warning(msg)
            logging.warning("Klio was unable to create this location.")
            logging.warning("Please create it yourself.")
            continue
        bucket_name = location.split("/")[2]
        _create_bucket_if_missing(bucket_name, storage_client, project)


def create_stackdriver_dashboard(context):
    project = context["pipeline_options"]["project"]
    job_name = context["job_name"]
    region = context["pipeline_options"]["region"]
    logging.info("Creating Stackdriver dashboard for job {}".format(job_name))
    return _create_stackdriver_group_if_missing(project, job_name, region)

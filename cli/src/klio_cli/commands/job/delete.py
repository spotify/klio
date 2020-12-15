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

import click
from google.cloud import pubsub_v1
from google.cloud import storage

from klio_cli.utils import stackdriver_utils as sd_utils


class DeleteJob(object):
    resource_types = ["topic", "subscription", "location"]

    def __init__(self, config):
        self.config = config
        self._pipeline_options = config.pipeline_options
        self._job_config = config.job_config

    def _confirmation_dialog(self, resource_type, name):
        yes_no = click.confirm(
            "Do you want to delete {} {}? ".format(resource_type, name)
        )
        if yes_no:
            confirmation = click.prompt(
                "Please confirm by typing out "
                "the name of the {}".format(resource_type)
            )
            if confirmation != name:
                raise ValueError(
                    "Invalid match. {} != {} "
                    "Are you sure you meant to delete the resource? "
                    "Please rerun this command and "
                    "choose 'No' next time.".format(confirmation, name)
                )
        else:
            return False
        return True

    def _get_resources(self):
        # Go through the list of resources set up
        # in input/output and ask for user input
        # on each one that we should delete.
        # The following are the types of resources we are considering:
        # 1. topics
        # 2. subscriptions
        # 3. buckets
        to_delete = {
            resource_type: [] for resource_type in self.resource_types
        }
        to_delete["stackdriver_group"] = False

        ev_inputs = self._job_config.events.inputs
        ev_outputs = self._job_config.events.outputs
        for resource in ev_inputs + ev_outputs:
            if "pubsub" == resource.name:
                if self._confirmation_dialog("topic", resource.topic):
                    to_delete["topic"].append(resource.topic)

                if not hasattr(resource, "subscription"):
                    continue
                if self._confirmation_dialog(
                    "subscription", resource.subscription
                ):
                    to_delete["subscription"].append(resource.subscription)

        data_inputs = self._job_config.data.inputs
        data_outputs = self._job_config.data.outputs
        for resource in data_inputs + data_outputs:
            if "gcs" == resource.name:
                if self._confirmation_dialog("location", resource.location):
                    to_delete["location"].append(resource.location)

        # Now lets handle the stackdriver group
        _, dashboard_name = sd_utils.generate_group_meta(
            self._pipeline_options.project,
            self.config.job_name,
            self._pipeline_options.region,
        )
        to_delete["stackdriver_group"] = self._confirmation_dialog(
            "stackdriver dashboard group", dashboard_name
        )

        return to_delete

    def _delete_subscriptions(self, subscriptions):
        if not subscriptions:
            return

        client = pubsub_v1.SubscriberClient()
        for subscription in subscriptions:
            logging.info("Deleting subscription {}".format(subscription))
            try:
                client.delete_subscription(
                    request={"subscription": subscription}
                )
            except Exception:
                logging.error(
                    "Failed to delete subscription {}".format(subscription),
                    exc_info=True,
                )

    def _delete_topics(self, topics):
        if not topics:
            return

        client = pubsub_v1.PublisherClient()
        for topic in topics:
            logging.info("Deleting topic {}".format(topic))
            try:
                client.delete_topic(request={"topic": topic})
            except Exception:
                logging.error(
                    "Failed to delete topic {}".format(topic), exc_info=True
                )

    def _delete_buckets(self, project, data_locations):
        if not data_locations:
            return

        client = storage.Client(project=project)
        for data_location in data_locations:
            logging.info("Deleting data_location {}".format(data_location))
            if data_location.startswith("gs://"):
                try:
                    client.get_bucket(data_location.split("/")[2]).delete(
                        force=True
                    )
                except Exception:
                    logging.error(
                        "Failed to delete bucket {}".format(data_location),
                        exc_info=True,
                    )
            else:
                logging.info(
                    "Skipping data location {}: Not a GCS location".format(
                        data_location
                    )
                )

    def delete(self):
        to_delete = self._get_resources()

        # Now we will go one by one through each resource and delete them all!
        self._delete_subscriptions(to_delete["subscription"])
        self._delete_topics(to_delete["topic"])
        self._delete_buckets(
            self._pipeline_options.project, to_delete["location"],
        )
        if to_delete["stackdriver_group"]:
            sd_utils.delete_stackdriver_group(
                self._pipeline_options.project,
                self.config.job_name,
                self._pipeline_options.region,
            )

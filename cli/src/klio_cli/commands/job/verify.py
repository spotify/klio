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

from urllib.parse import urlparse

from google.api_core import exceptions as api_ex
from google.cloud import exceptions
from google.cloud import pubsub_v1
from google.cloud import storage
from googleapiclient import discovery
from googleapiclient import errors as google_errors

from klio_cli.utils import stackdriver_utils as sd_utils


GCP_ROLE_METRIC_WRITER = "roles/monitoring.metricWriter"
#
# Add any extra roles we need to check on the default service account
# to this set:
#
ROLES_TO_CHECK = {GCP_ROLE_METRIC_WRITER}

logging.getLogger("googleapiclient.discovery").setLevel(logging.ERROR)


class VerifyJob(object):
    def __init__(self, klio_config, create_resources):
        self.klio_config = klio_config
        self.create_resources = create_resources
        self.project = self.klio_config.pipeline_options.project
        self.job_name = self.klio_config.job_name
        self.region = self.klio_config.pipeline_options.region

        self._storage_client = None
        self._publisher_client = None
        self._subscriber_client = None
        self._iam_client = None
        self._compute_client = None

    @property
    def storage_client(self):
        if self._storage_client is None:
            self._storage_client = storage.Client(self.project)
        return self._storage_client

    @property
    def publisher_client(self):
        if self._publisher_client is None:
            self._publisher_client = pubsub_v1.PublisherClient()
        return self._publisher_client

    @property
    def subscriber_client(self):
        if self._subscriber_client is None:
            self._subscriber_client = pubsub_v1.SubscriberClient()
        return self._subscriber_client

    @property
    def iam_client(self):
        if self._iam_client is None:
            self._iam_client = discovery.build(
                "cloudresourcemanager", "v1", cache_discovery=False
            )
        return self._iam_client

    @property
    def compute_client(self):
        if self._compute_client is None:
            self._compute_client = discovery.build(
                "compute", "v1", cache_discovery=False
            )
        return self._compute_client

    def _verify_stackdriver_dashboard(self):
        logging.info("Verifying Stackdriver Dashboard...")
        dashboard_url = None

        try:
            dashboard_url = sd_utils.get_stackdriver_group_url(
                self.project, self.job_name, self.region
            )
        # Raising a general exception (caught in verify_job) since we're unsure
        # of how to proceed
        except Exception as e:
            msg = (
                "Error encountered while trying to determine whether a "
                "Stackdriver dashboard already exists for this job: . "
                "{}".format(e)
            )
            logging.error(msg)
            raise e

        if dashboard_url:
            logging.info(
                "Stackdriver Dashboard exists at {}".format(dashboard_url)
            )
            return True

        elif self.create_resources:
            logging.info("Creating Stackdriver Dashboard...")
            # nothing already exists; we'll create one now
            dashboard_url = sd_utils.create_stackdriver_group(
                self.project, self.job_name, self.region
            )
            if dashboard_url:  # could still be none
                logging.info(
                    "Stackdriver Dashboard created at {}".format(dashboard_url)
                )
                return True
            # reason why dashboard couldn't be created will already be logged
            logging.error("Could not create Stackdriver Dashboard.")
        else:
            logging.info("No Stackdriver Dashboard exists.")
        return False

    def _verify_gcs_bucket(self, bucket):
        logging.info("Verifying {}".format(bucket))
        gcs_loc = urlparse(bucket)
        if not gcs_loc.scheme == "gs":
            logging.error(
                "Unsupported location type, skipping: {}".format(bucket)
            )
            return False
        try:
            if self.create_resources:
                self.storage_client.create_bucket(gcs_loc.netloc)
                logging.info("Creating your bucket {}...".format(bucket))
                logging.info("Successfully created your bucket.")
            else:
                self.storage_client.get_bucket(gcs_loc.netloc)
                logging.info("Bucket {} exists".format(bucket))
            return True
        except api_ex.Conflict:
            logging.info("Bucket {} exists".format(bucket))
            return True
        except exceptions.NotFound:
            logging.error("The bucket {} was not found.".format(bucket))
            return False
        except Exception as e:
            logging.error("Unable to verify {} due to: {}".format(bucket, e))
            return False

    def _verify_pub_topic(self, topic, _type):
        logging.info("Verifying your {} topic: {}".format(_type, topic))
        try:
            if self.create_resources:
                self.publisher_client.create_topic(request={"name": topic})
            else:
                self.publisher_client.get_topic(request={"topic": topic})
                logging.info("Topic {} exists".format(topic))
            return True
        except api_ex.AlreadyExists:
            logging.info("Topic {} exists".format(topic))
            return True
        except exceptions.NotFound:
            logging.error("The topic {} was not found.".format(topic))
            return False
        except Exception as e:
            logging.error(
                "Unable to verify the {} topic {} due to: {}".format(
                    _type, topic, e
                )
            )
            return False

    def _verify_subscription_and_topic(
        self, sub, upstream_topic=None,
    ):
        verified_sub = True
        logging.info(
            "Verifying your input topic and subscription: "
            "{} and {}".format(upstream_topic, sub)
        )

        if upstream_topic is None:
            logging.info(
                "You have no topic associated with the subscription {}.".format(
                    sub
                )
            )
            logging.info("Please create a topic for your subscription.")
            verified_topic = False
        else:
            verified_topic = self._verify_pub_topic(upstream_topic, "input")
            try:
                if self.create_resources:
                    self.subscriber_client.create_subscription(
                        request={"name": sub, "topic": upstream_topic}
                    )
                    logging.info(
                        "Creating the subscription {} to topic {}".format(
                            sub, upstream_topic
                        )
                    )
                    logging.info("Successfully created your subscription.")
                else:
                    self.subscriber_client.get_subscription(
                        request={"subscription": sub}
                    )
                    logging.info("Subscription %s exists" % sub)
            except api_ex.AlreadyExists:
                logging.info("Subscription {} exists".format(sub))
            except exceptions.NotFound:
                logging.error("The subscription {} was not found.".format(sub))
                verified_sub = False
            except Exception as e:
                logging.error(
                    "Unable to verify the input"
                    + "subscription {} due to: {}".format(sub, e)
                )
                verified_sub = False

        return verified_topic, verified_sub

    # TODO: This needs to be overhauled for v2.  Don't assume GCS, pubsub.
    def _verify_inputs(self):
        unverified_bucket_count = 0
        unverified_topic_count = 0
        unverified_sub_count = 0

        data_inputs = self.klio_config.job_config.data.inputs

        if not data_inputs:
            logging.warning("Your job has no data inputs, is that expected?")

        logging.info("Verifying your data inputs...")

        for _input in data_inputs:
            input_gcs = _input.location

            if input_gcs is not None:
                verified_bucket = self._verify_gcs_bucket(input_gcs)
                if not verified_bucket:
                    unverified_bucket_count += 1
            else:
                message = "There is no data_location for {}".format(_input)
                logging.error(message)

        event_inputs = self.klio_config.job_config.events.inputs

        if not event_inputs:
            logging.warning("Your job has no event inputs, is that expected?")

        logging.info("Verifying your event inputs...")

        pubsub_event_inputs = [
            _i for _i in event_inputs if _i.name == "pubsub"
        ]
        for _input in pubsub_event_inputs:
            input_topic = _input.topic
            input_subscription = _input.subscription

            if input_subscription is not None:
                (
                    unverified_topic,
                    unverified_sub,
                ) = self._verify_subscription_and_topic(
                    input_subscription, input_topic,
                )
                if not unverified_topic:
                    unverified_topic_count += 1
                if not unverified_sub:
                    unverified_sub_count += 1
            else:
                logging.error(
                    "There is no subscription associated with {}.".format(
                        _input
                    )
                )

        if len(pubsub_event_inputs):
            logging.info(
                "You have {} unverified input buckets, {} unverified input "
                "topics and {} unverified input subscriptions".format(
                    unverified_bucket_count,
                    unverified_topic_count,
                    unverified_sub_count,
                )
            )
        else:
            logging.info(
                "You have {} unverified input buckets".format(
                    unverified_bucket_count,
                )
            )

        if (
            unverified_bucket_count
            + unverified_topic_count
            + unverified_sub_count
            == 0
        ):
            return True
        return False

    def _verify_outputs(self):
        unverified_bucket_count = 0
        unverified_topic_count = 0

        data_outputs = self.klio_config.job_config.data.outputs

        logging.info("Verifying your data outputs...")

        for output in data_outputs:
            output_bucket = output.location

            if output_bucket is not None:
                verified_bucket = self._verify_gcs_bucket(output_bucket)
                if not verified_bucket:
                    unverified_bucket_count += 1
            else:
                logging.error(
                    "There is no data_location for {}".format(output)
                )

        event_outputs = self.klio_config.job_config.events.outputs

        logging.info("Verifying your event outputs...")

        pubsub_event_outputs = [
            _i for _i in event_outputs if _i.name == "pubsub"
        ]
        for output in pubsub_event_outputs:
            output_topic = output.topic

            if output_topic is not None:
                verified_topic = self._verify_pub_topic(output_topic, "output")
                if not verified_topic:
                    unverified_topic_count += 1
            else:
                logging.error("There is no topic for {}".format(output))

        if pubsub_event_outputs:
            logging.info(
                "You have {} unverified output buckets "
                "and {} unverified output "
                "topics".format(
                    unverified_bucket_count, unverified_topic_count
                )
            )
        else:
            logging.info(
                "You have {} unverified output buckets".format(
                    unverified_bucket_count
                )
            )
        if unverified_bucket_count + unverified_topic_count == 0:
            return True
        return False

    def _verify_tmp_files(self):
        staging_location = self.klio_config.pipeline_options.staging_location
        temp_location = self.klio_config.pipeline_options.temp_location
        unverified = []

        for bucket in [staging_location, temp_location]:
            verified_bucket = self._verify_gcs_bucket(bucket)
            unverified.append(verified_bucket)

        return all(unverified)

    def _verify_iam_roles(self):
        logging.info("Verifying IAM roles for the job's service account")

        service_account = (
            self.klio_config.pipeline_options.service_account_email
        )
        if not service_account:
            try:
                response = (
                    self.compute_client.projects()
                    .get(project=self.project)
                    .execute()
                )
            except google_errors.HttpError as e:
                logging.error(
                    "Error verifying IAM roles: could"
                    + " not get project information. "
                    "Skipping.",
                    exc_info=e,
                )
                return False
            service_account = response["defaultServiceAccount"]

        try:
            response = (
                self.iam_client.projects()
                .getIamPolicy(resource=self.project, body={})
                .execute()
            )
        except google_errors.HttpError as e:
            logging.error(
                "Error verifying IAM roles: could"
                + " not get IAM policies on job's "
                "service account. Skipping.",
                exc_info=e,
            )
            return False
        bindings = response.get("bindings", [])
        svc_account_string = "serviceAccount:{}".format(service_account)

        # Invert dictionary from role -> [svc account] to svc account -> [role]
        members_lookup = {}
        for el in bindings:
            role = el.get("role")
            for mem in el.get("members", []):
                members_lookup.setdefault(mem, set()).add(role)

        svc_account_roles = members_lookup.get(svc_account_string, set())
        if {"roles/editor", "roles/owner"}.intersection(svc_account_roles):
            logging.warning(
                "The default compute service account has "
                "unsafe project editor or owner permissions."
            )

        if ROLES_TO_CHECK.issubset(svc_account_roles):
            logging.info(
                "Verified that the service account has the required roles"
            )
            return True

        logging.error(
            "The default compute service account is missing the following IAM "
            "roles: {}".format(ROLES_TO_CHECK - svc_account_roles)
        )
        if self.create_resources:
            logging.error(
                "--create-resources is not able to add"
                " these roles to the service "
                "account at this time. Please add them manually via the Google "
                "Cloud console."
            )
        return False

    def _verify_all(self):
        verified_tmp_files = self._verify_tmp_files()
        verified_inputs = self._verify_inputs()
        verified_outputs = self._verify_outputs()
        verified_iam_roles = self._verify_iam_roles()
        verified_dashboard = self._verify_stackdriver_dashboard()

        return (
            verified_tmp_files
            and verified_inputs
            and verified_outputs
            and verified_dashboard
            and verified_iam_roles
        )

    def verify_job(self):
        try:
            verify_result = self._verify_all()

        except Exception as e:
            logging.error(
                "Unable to run the verify command due to: {}".format(e)
            )
            raise SystemExit(1)

        if verify_result:
            logging.info(
                "Good job! Your job inputs,"
                " outputs and config are verified and ready to go."
            )
        else:
            logging.error(
                "You have errors with your GCS resources. Please fix "
                "and try again."
            )
            raise SystemExit(1)

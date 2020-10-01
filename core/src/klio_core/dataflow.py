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
"""Module for interacting with the `Dataflow REST API
<https://cloud.google.com/dataflow/docs/reference/rest>`_."""

import functools
import logging
import random

from googleapiclient import discovery

from klio_core import utils
from klio_core import variables


class DataflowClient(object):
    """Client to interact with Dataflow REST API.

    Args:
        api_version (str): Version of Dataflow REST API. Defaults to
            ``v1b3``.
    """

    def __init__(self, api_version=None):
        _api_version = api_version or "v1b3"
        self.client = discovery.build("dataflow", _api_version)
        self.logger = logging.getLogger("klio")

    def find_job_by_name(self, job_name, gcp_project, region=None):
        """Search Dataflow for a job given its name and GCP project.

        Args:
            job_name (str): Name of Dataflow job.
            gcp_project (str): GCP project in which to search.
            region (str): Region in which to search. Defaults to
                searching all regions in
                :attr:`klio_core.variables.DATAFLOW_REGIONS`.
        Returns:
            dict or None: If found, ``dict`` of job summary results. Otherwise,
            ``None``.
        """
        if not region:
            regions = variables.DATAFLOW_REGIONS
        else:
            regions = (region,)

        base_request = self.client.projects().locations().jobs()

        all_matching_jobs = []

        # TODO: no batch requesting from Google's side, but should add
        #       threading to send multiple requests concurrently. @lynn
        for region in regions:
            # Note: the parameter `view="JOB_VIEW_ALL"` does not return
            #       the same information in this `.list()` call as it
            #       does in the `.get()` call in `get_job_detail` below.
            request = base_request.list(
                projectId=gcp_project, location=region, filter="ACTIVE"
            )

            try:
                response = request.execute()

            # general catch all since the handling would be the same no matter
            # of the exception
            except Exception as e:
                self.logger.warning(
                    "Error listing active jobs in project '%s' in region '%s':"
                    " %s" % (gcp_project, region, e)
                )
                continue

            job_results = response.get("jobs", [])
            if job_results:
                for result in job_results:
                    if result["name"] == job_name:
                        all_matching_jobs.append(result)

        # Note: job names are unique within regions, but not across
        #       regions :grimace:
        if len(all_matching_jobs) > 1:
            self.logger.info(
                "More than one parent job found for job name '%s' under "
                "project '%s'. Selecting one at random."
            )
            return random.choice(all_matching_jobs)
        if all_matching_jobs:
            return all_matching_jobs[0]

    def get_job_detail(self, job_name, gcp_project, region=None):
        """Get verbose job detail given a job name.

        Args:
            job_name (str): Name of Dataflow job.
            gcp_project (str): GCP project in which to search.
            region (str): Region in which to search. Defaults to
                searching all regions in
                :attr:`klio_core.variables.DATAFLOW_REGIONS`.
        Returns:
            dict or None: If found, ``dict`` of detailed job results.
            Otherwise, ``None``.
        """
        basic_job = self.find_job_by_name(job_name, gcp_project, region)
        if not basic_job:
            return None

        job_id = basic_job["id"]
        job_location = basic_job["location"]

        request = (
            self.client.projects()
            .locations()
            .jobs()
            .get(
                projectId=gcp_project,
                location=job_location,
                jobId=job_id,
                view="JOB_VIEW_ALL",
            )
        )
        try:
            response = request.execute()
        # general catch all since the handling would be the same no matter
        # of the exception
        except Exception as e:
            self.logger.warning(
                "Error getting job detail for '%s' in project '%s' in "
                "region '%s': %s" % (job_name, gcp_project, job_location, e)
            )
            return

        return response

    def get_job_input_topic(self, job_name, gcp_project, region=None):
        """Get input topic of a particular job.

        Args:
            job_name (str): Name of Dataflow job.
            gcp_project (str): GCP project in which to search.
            region (str): Region in which to search. Defaults to
                searching all regions in
                :attr:`klio_core.variables.DATAFLOW_REGIONS`.
        Returns:
            str or None: If found, input topic of job. Otherwise, ``None``.
        """
        job_info = self.get_job_detail(job_name, gcp_project, region=None)
        if not job_info:
            return None

        read_pubsub_user_name = "ReadFromPubSub/Read"

        for step in job_info.get("steps", []):
            if step.get("kind") == "ParallelRead":
                props = step.get("properties", {})
                user_name = props.get("user_name", {})
                if user_name.get("value") == read_pubsub_user_name:
                    # TODO: support multiple input topics; will need to
                    #       see how Google's response json renders it. @lynn
                    return props.get("pubsub_topic", {}).get("value")


def get_dataflow_client(api_version=None):
    """Get an initialized :class:`DataflowClient`.

    This function will first check if there is an already initialized
    client in the global namespace. Otherwise, initialize one then set it
    in the global namespace to avoid redundant initialization.

    Args:
        api_version (str): Version of Dataflow REST API. Defaults to
            ``v1b3``.
    Returns:
        DataflowClient: A client to interact with the Dataflow REST API.
    """
    if not api_version:
        api_version = "v1b3"
    key = "dataflow_client_{}".format(api_version)
    initializer = functools.partial(DataflowClient, api_version)
    return utils.get_or_initialize_global(key, initializer)

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

from google.cloud import monitoring


STACKDRIVER_GROUP_BASE_URL = "https://app.google.stackdriver.com/groups"
STACKDRIVER_GROUP_TPL = (
    "{base_url}/{group_id}/{display_name}?project={project}"
)
DASHBOARD_NAME_TPL = "{job_name}-{region}-klio-dashboard"


def generate_group_meta(project, job_name, region):
    name = "projects/{}".format(project)
    dashboard_name = DASHBOARD_NAME_TPL.format(
        job_name=job_name, region=region
    )
    return name, dashboard_name


# callees of this function should wrap in try/except since the call to
# client.list_groups does not
def get_stackdriver_group_url(project, job_name, region):
    client = monitoring.GroupServiceClient()
    name, dashboard_name = generate_group_meta(project, job_name, region)
    groups = client.list_groups(request={"name": name})

    for group in groups:
        if group.display_name == dashboard_name:
            group_id = group.name.split("/")[-1]
            return STACKDRIVER_GROUP_TPL.format(
                base_url=STACKDRIVER_GROUP_BASE_URL,
                group_id=group_id,
                display_name=dashboard_name,
                project=project,
            )


# NOTE: this will create a new group with the same name, rather than raise
# an "Already Exists" error.
def create_stackdriver_group(project, job_name, region):
    client = monitoring.GroupServiceClient()
    name, dashboard_name = generate_group_meta(project, job_name, region)
    group = {
        "display_name": dashboard_name,
        "filter": "resource.metadata.name=starts_with({})".format(job_name),
    }

    try:
        group = client.create_group(request={"name": name, "group": group})
    except Exception as e:
        msg = (
            "Could not create a Stackdriver for job '{}': {}. "
            "Skipping...".format(job_name, e)
        )
        logging.error(msg)
        return

    group_id = group.name.split("/")[-1]
    url = STACKDRIVER_GROUP_TPL.format(
        base_url=STACKDRIVER_GROUP_BASE_URL,
        group_id=group_id,
        display_name=dashboard_name,
        project=project,
    )
    msg = "Created dashboard '{}' for job '{}': {}".format(
        dashboard_name, job_name, url
    )
    logging.info(msg)
    return url


# Note: This will attempt to delete a group without retries.
def delete_stackdriver_group(project, job_name, region):
    client = monitoring.GroupServiceClient()
    name, dashboard_name = generate_group_meta(project, job_name, region)

    try:
        for group in client.list_groups(request={"name": name}):
            if group.display_name == dashboard_name:
                client.delete_group(
                    request={"name": group.name, "recursive": True}
                )
                msg = "Deleted dashboard '{}' for job '{}'".format(
                    dashboard_name, job_name
                )
                logging.info(msg)
                return
    except Exception as e:
        msg = (
            "Could not delete a Stackdriver for job '{}': {}. "
            "Skipping...".format(job_name, e)
        )
        logging.error(msg)
        return

    logging.warning(
        "No dashboard for job '{}' could be found. Nothing deleted".format(
            job_name
        )
    )

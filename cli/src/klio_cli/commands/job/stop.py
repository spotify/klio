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

import datetime
import logging
import time

import emoji

from googleapiclient import discovery


JOB_STATE_MAP = {"cancel": "JOB_STATE_CANCELLED", "drain": "JOB_STATE_DRAINED"}


class StopJob(object):
    def __init__(self, api_version=None):
        self._set_dataflow_client(api_version)

    def _set_dataflow_client(self, api_version):
        if not api_version:
            api_version = "v1b3"
        self._client = discovery.build("dataflow", api_version)

    def _check_job_running(self, job_name, project, region):
        request = (
            self._client.projects()
            .locations()
            .jobs()
            .list(projectId=project, location=region, filter="ACTIVE",)
        )

        try:
            response = request.execute()
        except Exception as e:
            logging.warning(
                "Could not find running job '{}' in project '{}': {}".format(
                    job_name, project, e
                )
            )
            logging.warning(
                "Continuing to attempt deploying '{}'".format(job_name)
            )
            return

        job_results = response.get("jobs", [])
        if job_results:
            for result in job_results:
                if result["name"] == job_name:
                    return result

    def _update_job_state(self, job, req_state=None, retries=None):
        if retries is None:
            retries = 0

        _req_state = JOB_STATE_MAP.get(req_state, JOB_STATE_MAP["cancel"])
        if job.get("requestedState") is not _req_state:
            job["requestedState"] = _req_state

        request = (
            self._client.projects()
            .locations()
            .jobs()
            .update(
                jobId=job["id"],
                projectId=job["projectId"],
                location=job["location"],
                body=job,
            )
        )

        try:
            request.execute()

        except Exception as e:
            # generic catch if 4xx error - probably shouldn't retry
            if getattr(e, "resp", None):
                if e.resp.status < 500:
                    msg = "Failed to {} job '{}': {}".format(
                        req_state, job["name"], e
                    )
                    logging.error(msg)
                    raise SystemExit(1)

            if retries > 2:
                msg = "Max retries reached: could not {} job '{}': {}".format(
                    req_state, job["name"], e
                )
                logging.error(msg)
                raise SystemExit(1)

            logging.info(
                "Failed to {} job '{}'. Trying again after 30s...".format(
                    req_state, job["name"]
                )
            )
            retries += 1
            time.sleep(30)
            self._update_job_state(job, req_state, retries)

    def _watch_job_state(self, job, timeout=600):
        timeout = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

        request = (
            self._client.projects()
            .locations()
            .jobs()
            .get(
                jobId=job["id"],
                projectId=job["projectId"],
                location=job["location"],
            )
        )

        while datetime.datetime.now() < timeout:
            try:
                resp = request.execute()
            except Exception as e:
                msg = (
                    "Failed to get current status for job '{}'. Error: {}.\n"
                    "Trying again after 5s...".format(job["name"], e)
                )
                logging.info(msg)
                time.sleep(5)
                continue

            if resp["currentState"] in JOB_STATE_MAP.values():
                return
            else:
                msg = "Waiting for job '{}' to reach terminal state...".format(
                    job["name"]
                )
                logging.info(msg)
                time.sleep(5)

        msg = "Job '{}' did not reach terminal state after '{}' secs.".format(
            job["name"], timeout
        )
        logging.error(msg)
        raise SystemExit(1)

    def stop(self, job_name, project, region, strategy, api_version=None):
        self._set_dataflow_client(api_version)
        current_running_job = self._check_job_running(
            job_name, project, region
        )

        if not current_running_job:
            return

        self._update_job_state(current_running_job, req_state=strategy)
        self._watch_job_state(current_running_job)
        verb = "cancelled" if strategy == "cancel" else "drained"
        msg = "Successfully {} job '{}' :smile_cat:".format(verb, job_name)
        logging.info(emoji.emojize(msg, use_aliases=True))

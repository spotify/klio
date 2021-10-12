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
"""Shared variables for use within the Klio ecosystem."""

import enum


# From https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
# Last pulled: 2020-10-01
# TODO: Figure out a way to get this dynamically (and maybe fall back to
#       hard-coded) so we don't have to manually keep this up to date.
DATAFLOW_REGIONS = (
    "asia-east1",
    "asia-northeast1",
    "asia-southeast1",
    "australia-southeast1",
    "europe-west1",
    "europe-west2",
    "europe-west3",
    "europe-west4",
    "northamerica-northeast1",
    "us-central1",
    "us-east1",
    "us-east4",
    "us-west1",
)
"""Default tuple of regions/locations for which to query."""


class KlioRunner(enum.Enum):
    DIRECT_GKE_RUNNER = "DirectGKERunner"
    DIRECT_RUNNER = "DirectRunner"
    DATAFLOW_RUNNER = "DataflowRunner"

    def __eq__(self, other):
        if not other.lower().endswith("runner"):
            other = other + "runner"
        if "direct" in other.lower():
            return self.value.lower() == other.lower()
        return other.lower() in self.value.lower()

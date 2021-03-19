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

import apache_beam as beam

import transforms


def run(input_pcol, config):
    """REQUIRED: Main entrypoint in running a job's transform(s).

    Any Beam transforms that need to happen after a message is consumed
    from PubSub from an upstream job, and before publishing a message to
    a downstream job (if needed/configured).

    Args:
        input_pcol: A Beam PCollection returned from
            ``beam.io.ReadFromPubSub``.
        config (klio.KlioConfig): Configuration as defined in
            ``klio-job.yaml``.
    Returns:
        apache_beam.pvalue.PCollection: PCollection that will be passed to
        the output transform for the configured event output (if any).
    """
    output_pcol = input_pcol | beam.ParDo(transforms.HelloKlio())
    # <-- multiple Klio-based ParDo transforms are supported here -->
    return output_pcol
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

import apache_beam as beam

import transforms


def run(input_pcol, config):
    """Main entrypoint in running a job's transform(s).

    Run any Beam transforms that need to happen after a message is
    consumed from PubSub from an upstream job (if not an apex job),
    and before  publishing a message to any downstream job (if
    needed/configured).

    Args:
        input_pcol: A Beam PCollection returned from
            ``beam.io.ReadFromPubSub``.
        config (klio.KlioConfig): Job-related configuration as
            defined in ``klio-job.yaml``.
    Returns:
        A Beam PCollection that will be passed to ``beam.io.WriteToPubSub``.
    """
    output_pcol = input_pcol | beam.ParDo(transforms.CatVDog())
    return output_pcol

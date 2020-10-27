# Copyright 2020 Spotify AB
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
"""Example of custom data existence checks implementation.

Please see the full walk-through here:
https://docs.klio.io/en/latest/userguide/
    examples/custom_data_ext_checks.html
"""

import apache_beam as beam

from apache_beam import pvalue
from apache_beam.io.gcp import gcsio

from klio.transforms import decorators


class BonjourInputCheck(beam.DoFn):
    @decorators.set_klio_context
    def setup(self):
        self.client = gcsio.Client()

    @decorators.handle_klio
    def process(self, data):
        element = data.element.decode("utf-8")

        ic = self._klio.config.job_config.data.inputs[0]
        subdirs = ("subdir1", "subdir2")

        inputs_exists = []

        for subdir in subdirs:
            path = f"{ic.location}/{subdir}/{element}.{ic.file_suffix}"
            exists = self.client.exists(path)
            inputs_exists.append(exists)

        if all(inputs_exists):
            yield data
        else:
            self._klio.logger.info(f"Skipping {element}: input data not found")


class BonjourOutputCheck(beam.DoFn):
    @decorators.set_klio_context
    def setup(self):
        self.client = gcsio.Client()

    @decorators.handle_klio
    def process(self, data):
        element = data.element.decode("utf-8")

        oc = self._klio.config.job_config.data.outputs[0]
        subdirs = ("subdir1", "subdir2")

        outputs_exist = []

        for subdir in subdirs:
            path = f"{oc.location}/{subdir}/{element}.{oc.file_suffix}"
            exists = self.client.exists(path)
            outputs_exist.append(exists)

        if all(outputs_exist):
            yield pvalue.TaggedOutput("not_found", data)
        else:
            yield pvalue.TaggedOutput("found", data)


#####
# example of run.py
#####
from klio.transforms import helpers

def run(input_pcol, config):
    output_data = input_pcol | beam.ParDo(BonjourOutputCheck()).with_outputs()

    output_force = output_data.found | helpers.KlioFilterForce()

    to_input_check = (
        (output_data.not_found, output_force.process)
        | beam.Flatten()
    )
    to_process = to_input_check | beam.ParDo(BonjourInputCheck())

    # continue on with the job-related logic
    output_pcol = to_process | ...
    return output_pcol

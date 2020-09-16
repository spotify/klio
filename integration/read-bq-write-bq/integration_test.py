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

# To be run after `klio job run --direct-runner` (not within job container)

import os
import unittest

import apache_beam as beam

from apache_beam.options import pipeline_options
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.testing import util as test_util
from apache_beam.testing import test_pipeline

from it import common

class TestExpectedOutput(unittest.TestCase):
    def test_is_equal(self):
        """The contents of the event input table are fed into the event output table"""
        klio_config = common.get_config()
        output_table_cfg = klio_config.job_config.events.outputs[0]
        output_table_spec = bigquery.TableReference(
            projectId=output_table_cfg.project,
            datasetId=output_table_cfg.dataset,
            tableId=output_table_cfg.table
        )

        options = {
            'project': output_table_cfg.project,
            'runner:': 'DirectRunner'
        }

        options = pipeline_options.PipelineOptions(flags=[], **options)
        with test_pipeline.TestPipeline(options=options) as p:
            actual_pcoll = p | "Actual" >> beam.io.Read(beam.io.BigQuerySource(output_table_spec))

            expected = [{"entity_id": v, "value": v} for v in common.entity_ids]
            test_util.assert_that(actual_pcoll, test_util.equal_to(expected))

if __name__ == '__main__':
    unittest.main()

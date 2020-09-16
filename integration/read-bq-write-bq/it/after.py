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
"""Cleanup after each integration test.

Deletes both input and output tables.
"""
import os

from apache_beam.io.gcp import bigquery_tools as beam_bq_tools

import common

def delete_tables():
    klio_cfg = common.get_config()
    input_table_cfg = klio_cfg.job_config.events.inputs[0]
    output_table_cfg = klio_cfg.job_config.events.outputs[0]

    bq_client = beam_bq_tools.BigQueryWrapper()

    bq_client._delete_table(input_table_cfg.project,
                            input_table_cfg.dataset,
                            input_table_cfg.table)

    bq_client._delete_table(output_table_cfg.project,
                            output_table_cfg.dataset,
                            output_table_cfg.table)

def restore_original_config():
    klio_cfg_file_path = os.path.join(os.path.dirname(__file__), "..", "klio-job.yaml")
    klio_save_file_path = os.path.join(os.path.dirname(__file__), "..", "klio-job.yaml.save")

    os.rename(klio_save_file_path, klio_cfg_file_path)

if __name__ == '__main__':
    delete_tables()
    restore_original_config()

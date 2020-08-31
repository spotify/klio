"""Setup for integration test

* Rewrites klio-job.yaml so that the input and output BigQuery tables have
names unique to the run
* Creates BigQuery tables that need to exist before the test run starts.
"""
import os

import apache_beam as beam
import yaml

import common


def _append_build_id(base_name):
    """Returns base_name with BQ-friendly `GITHUB_SHA` appended"""
    build_id = os.environ.get("GITHUB_SHA", None)

    if not build_id:
        raise Exception("Unable to get build id; env var GITHUB_SHA not set")

    # valid BQ table names only allow underscores and alphanumeric chars
    # https://cloud.google.com/bigquery/docs/tables#table_naming
    table_name = "{}_{}".format(base_name, build_id.replace("-", "_"))

    return table_name

def rewrite_klio_config_yaml():
    """Rewrite `klio-job.yaml` with tablenames that have the GITHUB_SHA appended"""
    klio_cfg_file_path = os.path.join(os.path.dirname(__file__), "..", "klio-job.yaml")
    klio_save_file_path = os.path.join(os.path.dirname(__file__), "..", "klio-job.yaml.save")

    with open(klio_cfg_file_path) as f:
        config_dict = yaml.safe_load(f)
        # save the original
        with open(klio_save_file_path, "w") as g:
            g.write(yaml.safe_dump(config_dict))

        new_input_table_name = _append_build_id(config_dict["job_config"]["events"]["inputs"][0]["table"])
        config_dict["job_config"]["events"]["inputs"][0]["table"] = new_input_table_name

        new_output_table_name = _append_build_id(config_dict["job_config"]["events"]["outputs"][0]["table"])
        config_dict["job_config"]["events"]["outputs"][0]["table"] = new_output_table_name

    with open(klio_cfg_file_path, "w") as g:
        g.write(yaml.safe_dump(config_dict))



def populate_bigquery_table():
    """Create & populate input table based on what is configured for event input in klio-job.yaml

    This needs to run before `klio job run` is called, which is why
    """
    table_schema = {"fields": [{
        'name': 'entity_id', 'type': 'STRING', 'mode': 'NULLABLE'
    }]}

    klio_cfg = common.get_config()
    input_table_cfg = klio_cfg.job_config.events.inputs[0]
    table_name = "{}:{}.{}".format(input_table_cfg.project,
                                   input_table_cfg.dataset,
                                   input_table_cfg.table)

    with beam.Pipeline() as p:
        def create_record(v):
            return {
                'entity_id': v,
            }

        record_ids = p | 'CreateIDs' >> beam.Create(common.entity_ids)
        records = record_ids | 'CreateRecords' >> beam.Map(lambda x: create_record(x))
        records | 'write' >> beam.io.WriteToBigQuery(
            table_name,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

if __name__ == '__main__':
    rewrite_klio_config_yaml()
    populate_bigquery_table()




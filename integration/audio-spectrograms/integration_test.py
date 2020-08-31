# Copyright 2020 Spotify AB

# To be run after `klio job run --direct-runner` (not within job container)

import os
import unittest
import yaml

from google.cloud import storage

from klio_core import config

HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
EXPECTED_LOGS = os.path.join(HERE, "expected_job_output.txt")
ACTUAL_LOGS = os.path.join(HERE, "job_output.log")
BATCH_IDS = os.path.join(HERE, "batch_track_ids.txt")


class TestExpectedOutput(unittest.TestCase):
    @classmethod
    def _load_klio_config(cls):
        config_file_path = os.path.join(os.path.dirname(__file__), "klio-job.yaml")
        with open(config_file_path) as f:
            return config.KlioConfig(yaml.safe_load(f))


    @classmethod
    def setUpClass(self):
        config = self._load_klio_config()
        self.gcs_bucket, self.gcs_object_path = os.path.split(config.job_config.data.inputs[0].location)
        self.gcs_bucket = self.gcs_bucket.split("/")[-1]
        self.project = config.pipeline_options.project
        self.client = storage.Client(project=self.project)

        self.output_dir = "/".join(config.job_config.data.outputs[0].location.split("/")[3:])

        with open(EXPECTED_LOGS, "r") as f:
            self.expected_logs = f.readlines()

        if not os.path.exists(ACTUAL_LOGS):
            # tox deletes the file after the test is done so that tests
            # don't pass accidentally from a previously successful run/
            # cached results
            raise Exception(
                "The job's output does not exist. Rerun the job to produce "
                "the required output."
            )

        with open(ACTUAL_LOGS, "r") as f:
            self.actual_logs = f.readlines()

        with open(BATCH_IDS, "r") as f:
            self.batch_ids = f.readlines()

        suffixes = ["full.png", "background.png", "foreground.png"]
        self.exp_files = ["-".join([i.strip(), s]) for i in self.batch_ids for s in suffixes]



    def test_expected_output(self):
        # sort them since the order of some parts of the pipeline are not
        # deterministic
        self.assertEqual(sorted(self.expected_logs), sorted(self.actual_logs))

    def _exists(self, expected_blob):
        bucket = self.client.lookup_bucket(self.gcs_bucket)
        if not bucket:
            return False

        object_path = os.path.join(self.output_dir, expected_blob)
        blob = bucket.get_blob(object_path)
        return blob is not None

    def test_expected_gcs_files(self):
        for exp_file in self.exp_files:
            exists = self._exists(exp_file)
            self.assertTrue(exists, msg=f"{exp_file} does not exist")

    @classmethod
    def tearDownClass(self):
        bucket = self.client.get_bucket(self.gcs_bucket)

        for exp_file in self.exp_files:
            object_path = os.path.join(self.output_dir, exp_file)
            bucket.delete_blob(object_path)


if __name__ == '__main__':
    unittest.main()

# Copyright 2020 Spotify AB

# To be run after `klio job run --direct-runner` (not within job container)

import os
import unittest


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIRST_INPUT = os.path.join(HERE, "batch_track_ids_1.txt")
SECOND_INPUT = os.path.join(HERE, "batch_track_ids_2.txt")
OUTPUT_FILE = os.path.join(HERE, "batch_track_ids_output-00000-of-00001")
EXPECTED_LOGS = os.path.join(HERE, "expected_job_output.txt")
ACTUAL_LOGS = os.path.join(HERE, "job_output.log")


class TestExpectedOutput(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        with open(FIRST_INPUT, "r") as f:
            self.expected_output = f.readlines()
        with open(SECOND_INPUT, "r") as f:
            self.expected_output.extend(f.readlines())

        if not os.path.exists(OUTPUT_FILE):
            # Let's delete the file after the test is done so that tests
            # don't pass accidentally from a previously successful run/
            # cached results
            raise Exception(
                "The job's output does not exist. Rerun the job to produce "
                "the required output."
            )

        with open(OUTPUT_FILE, "r") as f:
            self.actual_output = f.readlines()

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

    def test_expected_output(self):
        self.assertEqual(
            sorted(self.expected_output), sorted(self.actual_output)
        )

    def test_expected_logs(self):
        # sort them since the order of some parts of the pipeline are not
        # deterministic
        self.assertEqual(sorted(self.expected_logs), sorted(self.actual_logs))

    @classmethod
    def tearDownClass(self):
        os.remove(OUTPUT_FILE)



if __name__ == "__main__":
    unittest.main()

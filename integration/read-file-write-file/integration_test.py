# Copyright 2020 Spotify AB

# To be run after `klio job run --direct-runner` (not within job container)

import os
import unittest


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
INPUT_FILE = os.path.join(HERE, "batch_track_ids.txt")
OUTPUT_FILE = os.path.join(HERE, "batch_track_ids_output-00000-of-00001")


class TestExpectedOutput(unittest.TestCase):
    def setUp(self):
        with open(INPUT_FILE, "r") as f:
            self.expected_output = f.readlines()

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

    def test_is_equal(self):
        self.assertEqual(self.expected_output, self.actual_output)

    def tearDown(self):
        os.remove(OUTPUT_FILE)



if __name__ == '__main__':
    unittest.main()

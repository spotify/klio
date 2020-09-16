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


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
EXPECTED_OUTPUT = os.path.join(HERE, "batch_track_ids_expected_output")
EXPECTED_LOGS = os.path.join(HERE, "expected_job_output.txt")
ACTUAL_OUTPUT = os.path.join(HERE, "batch_track_ids_output-00000-of-00001")
ACTUAL_LOGS = os.path.join(HERE, "job_output.log")


class TestExpectedOutput(unittest.TestCase):
    def setUp(self):
        with open(EXPECTED_OUTPUT, "r") as f:
            self.expected_output = f.readlines()

        with open(EXPECTED_LOGS, "r") as f:
            self.expected_logs = f.readlines()

        if not os.path.exists(ACTUAL_OUTPUT) or not os.path.exists(ACTUAL_LOGS):
            # Let's delete the file after the test is done so that tests
            # don't pass accidentally from a previously successful run/
            # cached results
            raise Exception(
                "The job's output does not exist. Rerun the job to produce "
                "the required output."
            )

        with open(ACTUAL_OUTPUT, "r") as f:
            self.actual_output = f.readlines()

        with open(ACTUAL_LOGS, "r") as f:
            self.actual_logs = f.readlines()

    def test_expected_output(self):
        self.assertEqual(self.expected_output, self.actual_output)
        self.assertEqual(self.expected_logs, self.actual_logs)

    def tearDown(self):
        os.remove(ACTUAL_OUTPUT)
        # ACTUAL_LOGS file is removed in tox


if __name__ == '__main__':
    unittest.main()

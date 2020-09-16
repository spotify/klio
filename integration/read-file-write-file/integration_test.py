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

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

import pytest

from klio_exec.commands.audit_steps import multithreaded_tf


@pytest.mark.parametrize("tf_loaded", (True, False))
@pytest.mark.parametrize("worker_threads", (0, 1, 2))
def test_multithreaded_tf_usage(
    tf_loaded, worker_threads, klio_config, mock_emit_warning, mocker
):
    if worker_threads:
        klio_config.pipeline_options.experiments = [
            "worker_threads={}".format(worker_threads)
        ]

    if tf_loaded:
        mocker.patch.dict("sys.modules", {"tensorflow": ""})

    mt_tf_usage = multithreaded_tf.MultithreadedTFUsage(
        "job/dir", klio_config, "term_writer"
    )

    mt_tf_usage.after_tests()

    if worker_threads != 1 and tf_loaded:
        # don't care about the actual message
        assert 1 == mock_emit_warning.call_count
    else:
        mock_emit_warning.assert_not_called()

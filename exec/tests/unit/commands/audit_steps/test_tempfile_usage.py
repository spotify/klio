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

import tempfile

from klio_exec.commands.audit_steps import tempfile_usage


def test_tempfile_usage(klio_config, mock_emit_error, mocker, monkeypatch):
    inst = tempfile_usage.TempFileUsage("job/dir", klio_config, "tw")

    assert "TemporaryFile" == tempfile.TemporaryFile.__name__  # sanity check

    inst.before_tests()

    assert "MockTemporaryFile" == tempfile.TemporaryFile.__name__

    assert inst._tempfile_used is False
    inst.after_tests()
    mock_emit_error.assert_not_called()

    with tempfile.TemporaryFile():
        pass

    assert inst._tempfile_used is True

    inst.after_tests()
    # don't really care about message, just that it was called
    assert 1 == mock_emit_error.call_count

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

from klio_exec.commands.audit_steps import base


class DummyAuditStep(base.BaseKlioAuditStep):
    AUDIT_STEP_NAME = "dummy_step"

    @staticmethod
    def get_description():
        return "A description of a dummy step!"

    def after_tests(self):
        pass


class IncompleteAuditStep(base.BaseKlioAuditStep):
    AUDIT_STEP_NAME = "incomplete_dummy_step"


class NotAnAuditStep(object):
    def after_tests(self):
        pass


TB_PYTEST = [
    'File "/usr/local/lib/python3.6/site-packages/pluggy/callers.py", line 187, in _multicall\n  res = hook_impl.function(*args)',  # NOQA: E501
    'File "/usr/local/lib/python3.6/site-packages/_pytest/python.py", line 178, in pytest_pyfunc_call\n  testfunction(**testargs)',  # NOQA: E501
    'File "/usr/src/app/test_transform.py", line 13, in test_process\n  assert expected == list(output)[0]',  # NOQA: E501
    'File "/usr/src/app/transforms.py", line 21, in process\n  with tempfile.TemporaryFile() as t:',  # NOQA: E501
]
TB_NO_PYTEST = [
    'File "/usr/src/app/transforms.py", line 21, in process\n  with tempfile.TemporaryFile() as t:'  # NOQA: E501
]


@pytest.mark.parametrize("tb,exp_len", ((TB_PYTEST, 2), (TB_NO_PYTEST, 1)))
def test_remove_all_frames_until_after_pytest(tb, exp_len):
    act_ret = base._get_relevant_frames(tb)

    assert exp_len == len(act_ret)


def test_base_klio_audit_step(mock_terminal_writer):
    assert issubclass(DummyAuditStep, base.BaseKlioAuditStep)
    assert issubclass(IncompleteAuditStep, base.BaseKlioAuditStep)
    assert not issubclass(NotAnAuditStep, base.BaseKlioAuditStep)

    dummy_inst = DummyAuditStep("job/dir", "config", mock_terminal_writer)
    assert dummy_inst.before_tests() is None
    # just making sure this doesn't raise
    dummy_inst.after_tests()
    assert dummy_inst.get_description() is not None

    inc_inst = IncompleteAuditStep("job/dir", "config", mock_terminal_writer)
    assert inc_inst.before_tests() is None
    assert inc_inst.get_description() is None
    with pytest.raises(NotImplementedError):
        inc_inst.after_tests()


@pytest.mark.parametrize("tb", (None, ["a traceback"]))
def test_emit(tb, mock_terminal_writer, mocker, monkeypatch):
    mock_format_list = mocker.Mock()
    mock_format_list.return_value = tb
    monkeypatch.setattr(base.traceback, "format_list", mock_format_list)
    dummy_inst = DummyAuditStep("job/dir", "config", mock_terminal_writer)

    msg = "Emit this message"
    exp_msg = "[dummy_step]: Emit this message\n"
    if tb:
        exp_msg = "{}{}\n".format(exp_msg, tb[0])

    kwargs = {"foo": "bar"}

    assert dummy_inst.warned is False  # sanity check
    dummy_inst.emit_warning(msg, tb=tb, **kwargs)

    if tb:
        mock_format_list.assert_called_once_with(tb)
    else:
        mock_format_list.assert_not_called()
    exp_kwargs = {"foo": "bar", "yellow": True}
    mock_terminal_writer.write.assert_called_once_with(exp_msg, **exp_kwargs)
    assert dummy_inst.warned is True

    mock_format_list.reset_mock()
    mock_terminal_writer.reset_mock()

    assert dummy_inst.errored is False  # sanity check
    dummy_inst.emit_error(msg, tb=tb, **kwargs)

    if tb:
        mock_format_list.assert_called_once_with(tb)
    else:
        mock_format_list.assert_not_called()
    exp_kwargs = {"foo": "bar", "red": True}
    mock_terminal_writer.write.assert_called_once_with(exp_msg, **exp_kwargs)
    assert dummy_inst.errored is True

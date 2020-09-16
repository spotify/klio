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

from klio_exec.commands import audit


@pytest.fixture
def mock_pytest(mocker, monkeypatch):
    mock_pt = mocker.Mock()
    monkeypatch.setattr(audit, "pytest", mock_pt)
    return mock_pt


@pytest.fixture
def mock_get_audit_steps(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(audit, "_get_audit_steps", mock)
    return mock


@pytest.fixture
def mock_run_pytest(mocker, monkeypatch):
    mock = mocker.Mock(return_value=False)
    monkeypatch.setattr(audit, "_run_pytest", mock)
    return mock


@pytest.mark.parametrize(
    "exit_code,exp_pytest_failed,exp_msg,exp_kwargs",
    (
        (1, True, "PyTest failed!\n", {"yellow": True}),
        (0, False, "PyTest passed!\n", {}),
    ),
)
def test_run_pytest(
    exit_code,
    exp_pytest_failed,
    exp_msg,
    exp_kwargs,
    mock_pytest,
    mock_terminal_writer,
    mocker,
):
    mock_pytest.main.return_value = exit_code
    actual_pytest_failed = audit._run_pytest(mock_terminal_writer)

    assert exp_pytest_failed == actual_pytest_failed
    exp_calls = [
        mocker.call("Running tests for audit validation...\n"),
        mocker.call(exp_msg, **exp_kwargs),
    ]
    mock_terminal_writer.write.assert_has_calls(exp_calls)


def test_get_audit_steps(mock_terminal_writer, mocker, monkeypatch):
    mock_steps = [mocker.Mock() for i in range(3)]
    mock_get_plugins = mocker.Mock()
    mock_get_plugins.return_value = mock_steps
    monkeypatch.setattr(
        audit.plugin_utils, "load_plugins_by_namespace", mock_get_plugins
    )

    ret = audit._get_audit_steps("job/dir", "config", mock_terminal_writer)

    mock_get_plugins.assert_called_once_with("klio.plugins.audit")
    assert ret == [s.return_value for s in mock_steps]
    for s in mock_steps:
        s.assert_called_once_with("job/dir", "config", mock_terminal_writer)


@pytest.mark.parametrize("param_value", (None, "something"))
@pytest.mark.parametrize("res_parsing", (True, False))
def test_list_audit_steps(
    res_parsing, param_value, mock_terminal_writer, mocker, monkeypatch
):
    mock_click_context = mocker.Mock()
    mock_click_context.resilient_parsing = res_parsing
    mock_print_plugins = mocker.Mock()
    monkeypatch.setattr(
        audit.plugin_utils, "print_plugins", mock_print_plugins
    )

    audit.list_audit_steps(mock_click_context, None, param_value)

    if not param_value or res_parsing:
        mock_terminal_writer.sep.assert_not_called()
        mock_print_plugins.assert_not_called()
        mock_click_context.exit.assert_not_called()
    else:
        mock_terminal_writer.sep.assert_called_once_with(
            "=", "Installed audit steps"
        )
        mock_print_plugins.assert_called_once_with(
            "klio.plugins.audit", mock_terminal_writer
        )
        mock_click_context.exit.assert_called_once_with()


@pytest.mark.parametrize("steps_passed", (0, 1))
@pytest.mark.parametrize("steps_warned", (0, 1))
def test_audit_steps(
    steps_passed,
    steps_warned,
    mock_terminal_writer,
    mock_get_audit_steps,
    mock_run_pytest,
    mocker,
):

    mock_passed = []
    for i in range(steps_passed):
        name = "PassedStep{}".format(i + 1)
        mock_passed.append(
            mocker.Mock(AUDIT_STEP_NAME=name, warned=False, errored=False)
        )

    mock_warned = []
    for i in range(steps_warned):
        name = "WarnStep{}".format(i + 1)
        mock_warned.append(
            mocker.Mock(AUDIT_STEP_NAME=name, warned=True, errored=False)
        )

    mock_audit_steps = mock_passed + mock_warned
    mock_get_audit_steps.return_value = mock_audit_steps

    audit.audit("job/dir", "config")

    mock_get_audit_steps.assert_called_once_with(
        "job/dir", "config", mock_terminal_writer
    )
    for step in mock_audit_steps:
        step.before_tests.assert_called_once_with()

    mock_run_pytest.assert_called_once_with(mock_terminal_writer)

    # don't really care about the message strings, just that these funcs have
    # been called
    assert 2 == mock_terminal_writer.sep.call_count
    assert len(mock_passed) + 2 == mock_terminal_writer.write.call_count


def test_audit_steps_errored(
    mock_terminal_writer, mock_get_audit_steps, mock_run_pytest, mocker
):
    mock_errored = []
    for i in range(2):
        name = "WarnStep{}".format(i + 1)
        mock_errored.append(
            mocker.Mock(AUDIT_STEP_NAME=name, warned=False, errored=True)
        )

    mock_get_audit_steps.return_value = mock_errored

    with pytest.raises(SystemExit):
        audit.audit("job/dir", "config")

    mock_get_audit_steps.assert_called_once_with(
        "job/dir", "config", mock_terminal_writer
    )
    for step in mock_errored:
        step.before_tests.assert_called_once_with()

    mock_run_pytest.assert_called_once_with(mock_terminal_writer)

    # don't really care about the message strings, just that these funcs have
    # been called
    assert 3 == mock_terminal_writer.sep.call_count
    assert 1 == mock_terminal_writer.write.call_count


def test_audit_raises(mock_get_audit_steps, mock_terminal_writer, caplog):
    mock_get_audit_steps.side_effect = Exception("Blah")

    with pytest.raises(SystemExit) as exc_info:
        audit.audit("job/dir", "config")

    assert 1 == exc_info.value.code
    # don't really care about the message strings, just that these funcs have
    # been called
    assert 1 == mock_terminal_writer.write.call_count
    assert 1 == len(caplog.records)

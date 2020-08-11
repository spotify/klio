# Copyright 2020 Spotify AB

import tempfile

from klio_exec.commands.audit_steps import tempfile_usage


def test_tempfile_usage(mocker, monkeypatch):
    mock_emit_error = mocker.Mock()
    monkeypatch.setattr(
        tempfile_usage.TempFileUsage, "emit_error", mock_emit_error
    )

    inst = tempfile_usage.TempFileUsage("job/dir", "config", "tw")

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

# Copyright 2020 Spotify AB

from klio_cli.utils import multi_line_terminal_writer


def test_simple_output(capsys):
    writer = multi_line_terminal_writer.MultiLineTerminalWriter()
    writer.emit_line("line one", "initial contents of line one")
    captured = capsys.readouterr()
    assert "initial contents of line one\n" == captured.out


def test_multiline_output(capsys):
    writer = multi_line_terminal_writer.MultiLineTerminalWriter()
    writer.emit_line("line one", "initial contents of line one")
    writer.emit_line("line two", "initial contents of line two")
    writer.emit_line("line one", "updated line one")
    captured = capsys.readouterr()
    expected_output = (
        "initial contents of line one\n"  # Write the first line
        "initial contents of line two\n"  # Write the second line
        "\x1b[2F"  # Move up two lines
        "updated line one\u001b[0K\n"  # Overwrite the first line
        "\x1b[1E"  # Move back down one line (as we already wrote a newline)
    )
    assert expected_output == captured.out

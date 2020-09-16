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

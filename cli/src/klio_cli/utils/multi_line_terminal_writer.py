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

from contextlib import contextmanager


CLEAR_TO_END_OF_LINE = "\u001b[0K"


def _move_n_lines(to_move: int) -> None:
    if to_move == 0:
        return
    # F to move the cursor up w/i the console; E to move
    # the cursor down
    ansi_code = "E" if to_move > 0 else "F"
    print("\033[%d%c" % (abs(to_move), ansi_code), end="", flush=True)


class MultiLineTerminalWriter(object):
    """
    A class to manage writing multi-line output to the terminal,
    with each line tagged with a line_id. Useful for displaying
    progress of multiple conncurrent items, like a multi-layer
    Docker push.
    """

    def __init__(self):
        self._line_id_to_index = {}
        self._lines_printed = 0

    def emit_line(self, line_id, contents):
        """
        Emit a string at the given line_id. If this line_id has not yet
        been printed, it will be placed at the bottom of the current
        terminal output.
        """
        writing_new_line_at_bottom = line_id not in self._line_id_to_index
        if writing_new_line_at_bottom:
            self._line_id_to_index[line_id] = len(self._line_id_to_index)
        with self._at_line(self._line_id_to_index[line_id]):
            if not writing_new_line_at_bottom:
                contents += CLEAR_TO_END_OF_LINE
            print(contents, flush=True)

    @contextmanager
    def _at_line(self, index):
        """
        A context manager that moves the cursor to a given line, assuming
        that the cursor is already placed at the bottom of the list of lines,
        and returns the cursor to the bottom after writing one line.
        """

        distance_from_bottom = self._lines_printed - index
        if distance_from_bottom:
            _move_n_lines(-distance_from_bottom)

        # Print one line.
        yield

        # Assume that a newline was printed when we yielded.
        # If we had to move up to get to this line, we have to move down
        # by one fewer line. If we didn't have to move up to get to this
        # line, we don't need to move down at all as we're already at
        # the new bottom.
        if distance_from_bottom != 0:
            distance_from_bottom -= 1
            _move_n_lines(distance_from_bottom)

        self._lines_printed = max(self._lines_printed, index + 1)

# Copyright 2021 Spotify AB
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

import code
import rlcompleter
import readline
import sys

from klio_exec.commands.utils import console_utils


# Note: this is defined here rather than in console_utils because we don't
# want to expose unnecessary variables when calling `globals()` that are 
# defined in the console_utils module
class KlioConsole(code.InteractiveConsole):
    def __init__(self, *args, **kwargs):
        super(KlioConsole, self).__init__(*args, **kwargs)
        self.init_autocomplete()

    def runcode(self, code):
        # Need to overwrite `runcode` because it does not pass in `globals()`
        # into the `exec` function. Access to globals are needed if someone
        # were to define a transform in the console and wanted to apply it
        # to a pipeline (otherwise it would not be accessible)
        try:
            exec(code, globals(), self.locals)
        except SystemExit:
            raise
        except:
            self.showtraceback()

    def init_autocomplete(self):
        # the completer use for autocompletion in custom consoles don't
        # have access to local variables, so we create our own completer
        # with the console's local vars
        # https://stackoverflow.com/a/12433619
        completer = rlcompleter.Completer(self.locals)
        readline.set_completer(completer.complete)


def start(config, meta, image_tag, runtime_conf, job_console_config):
    sys.ps1 = console_utils.PS1
    sys.ps2 = console_utils.PS2
    ctx_mgr = console_utils.KlioConsoleContextManager(
        config.job_name, 
        config, 
        runtime_conf, 
        job_console_config=job_console_config
    )
    header = ctx_mgr.get_header_with_logo()
    scope_vars = ctx_mgr.get_local_scope()
    console = KlioConsole(locals=scope_vars)
    console.interact(header, console_utils.EXIT_MSG) 
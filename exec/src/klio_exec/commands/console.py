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
import pickle
import rlcompleter
import readline
import sys

from IPython.terminal import embed
from notebook import notebookapp

from klio_exec.commands.utils import interactive_utils
from klio_exec.commands.utils import interactive_common as ic
from klio_exec.commands.utils import ipython_ext


# Note: this is defined here rather than in interactive_utils because we don't
# want to expose unnecessary variables when calling `globals()` that are 
# defined in the interactive_utils module
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


def _start_repl(config, meta, image_tag, runtime_conf, job_console_config):
    sys.ps1 = ic.REPL_PROMPT1
    sys.ps2 = ic.REPL_PROMPT2
    ctx_mgr = interactive_utils.InteractiveKlioContext()
    banner = ctx_mgr.get_banner()
    scope_vars = ctx_mgr.get_local_scope_objects()
    console = KlioConsole(locals=scope_vars)
    console.interact(banner, ic.EXIT_MSG)


def _start_ipython_repl(config, meta, image_tag, runtime_conf, job_console_config):
    ctx_mgr = interactive_utils.InteractiveKlioContext()
    banner = ctx_mgr.get_banner()
    scope_vars = ctx_mgr.get_local_scope_objects()
    ipython_repl = embed.InteractiveShellEmbed()
    ipython_repl(header=banner, local_ns=scope_vars)


def _start_notebook(config, meta, image_tag, runtime_conf, job_console_config):
    ipython_ext.generate_ipython_config()
    port = "8888"  # TODO: make configurable
    init_args = [
        "--port", port, 
        "--ip", "0.0.0.0",  # makes jupyter nb accessible from host thru container
        "--allow-root",  # docker runs as root
        "--no-browser",  # don't launch browser while in the docker container
    ]

    app = notebookapp.NotebookApp()
    app.initialize(init_args)
    app.start()


def start(config, meta, image_tag, runtime_conf, job_console_config):
    if job_console_config.notebook:
        return _start_notebook(config, meta, image_tag, runtime_conf, job_console_config)
    if job_console_config.ipython:
        return _start_ipython_repl(config, meta, image_tag, runtime_conf, job_console_config)
    return _start_repl(config, meta, image_tag, runtime_conf, job_console_config)

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
#
"""TODO: docstrings"""

import code
import collections
import sys

import apache_beam as beam
from apache_beam import __version__ as beam_version
from apache_beam.options import pipeline_options

from klio import __version__ as klio_version
from klio.transforms import core
from klio_core import __version__ as klio_core_version
from klio_exec import __version__ as klio_exec_version
from klio_exec.commands import run
from klio_exec.commands.utils import interactive_common as ic

_VER_INF = sys.version_info
PY_VERSION = f"{_VER_INF[0]}.{_VER_INF[1]}.{_VER_INF[2]}"

# TODO: fixme
try:
    import readline
except ImportError:
    print("Module readline not available.")
else:
    import rlcompleter
    readline.parse_and_bind("tab: complete")


# via https://stackoverflow.com/a/1653978
class OrderedSet(collections.OrderedDict, collections.MutableSet):
    """TODO: docstrings"""
    def update(self, *args, **kwargs):
        if kwargs:
            raise TypeError("update() takes no keyword arguments")

        for s in args:
            for e in s:
                 self.add(e)

    def add(self, elem):
        self[elem] = None

    def discard(self, elem):
        self.pop(elem, None)

    def __le__(self, other):
        return all(e in other for e in self)

    def __lt__(self, other):
        return self <= other and self != other

    def __ge__(self, other):
        return all(e in self for e in other)

    def __gt__(self, other):
        return self >= other and self != other

    def __repr__(self):
        return 'OrderedSet([%s])' % (', '.join(map(repr, self.keys())))

    def __str__(self):
        return '{%s}' % (', '.join(map(repr, self.keys())))
    
    # difference = __sub__ 
    # difference_update = __isub__
    # intersection = __and__
    # intersection_update = __iand__
    # issubset = __le__
    # issuperset = __ge__
    # symmetric_difference = __xor__
    # symmetric_difference_update = __ixor__
    # union = __or__


class KlioPipelineWrapper(beam.Pipeline):
    """Light Klio wrapper around :class:`beam.Pipeline` object.
    
    Only meant for interactive job console use via `klio job console`.
    """
    def __init__(self, *args, **kwargs):
        super(KlioPipelineWrapper, self).__init__(*args, **kwargs)
        self._klio_transform_stack = OrderedSet()

    def get_transforms(self):
        return self._klio_transform_stack

    def apply(self, transform, *args, **kwargs):
        self._klio_transform_stack.add(transform)
        return super(KlioPipelineWrapper, self).apply(transform, *args, **kwargs)

    def __repr__(self):
        # TODO: what to name this? prob not with `id` - maybe something more recognizable
        return f"<KlioInteractiveBeamPipeline(id={id(self)})>"


class KlioConsoleContextManager(run.KlioPipeline):
    """TODO: docstrings"""

    def __init__(self, *args, job_console_config=None, **kwargs):
        super(KlioConsoleContextManager, self).__init__(*args, **kwargs)
        self.job_console_config = job_console_config

    def __parse_runner(self, runner):
        supported_runners = (
            "dataflowrunner", "directrunner", "dataflow", "direct",
            "interactive", "interactiverunner"
        )
        if runner is None:
            runner = "DirectRunner"

        elif runner.lower() not in supported_runners:
            raise Exception(f"Unrecognized runner: {runner}")

        else:
            runner = {
                "dataflowrunner": "DataflowRunner",
                "dataflow": "DataflowRunner",
                "directrunner": "DirectRunner",
                "direct": "DirectRunner",
                "interactive": "InteractiveRunner",
                "interactiverunner": "InteractiveRunner",
            }[runner.lower()]
        return runner

    def get_new_pipeline(self, runner=None, options=None):
        if options is None:
            options = self._get_pipeline_options()
            options.view_as(pipeline_options.SetupOptions).save_main_session = True

        runner = self.__parse_runner(runner)
        options.view_as(pipeline_options.StandardOptions).runner = runner
        return KlioPipelineWrapper(options=options)

    def get_original_pipeline(self, runner=None):
        options = self._get_pipeline_options()
        options.view_as(pipeline_options.SetupOptions).save_main_session = True
        runner = self.__parse_runner(runner)
        options.view_as(pipeline_options.StandardOptions).runner = runner
        beam_pipeline = KlioPipelineWrapper(options=options)
        self._setup_pipeline(beam_pipeline)
        return beam_pipeline

    def get_original_pipeline_options(self):
        return self._get_pipeline_options()

    def get_new_pipeline_options(self, config_dict=None):
        if config_dict is None:
            config_dict = {}
        # TODO: check to see if there's pipeline_options already in keys
        return pipeline_options.PipelineOptions().from_dictionary(config_dict)

    def get_run_transforms(self):
        return self._get_run_callable()

    def get_local_scope(self):
        import klio
        # NOTE: When adding more to the scope, be sure to add it to the 
        # relevant _SCOPE_* constants in `interactive_common.py` so that 
        # it's described when the console loads for user
        return {
            # variables
            "kpipeline": self.get_original_pipeline(),
            "kctx": core.KlioContext(),  # TODO: maybe move to a method?
            # functions
            "get_new_pipeline": self.get_new_pipeline,
            "get_new_pipeline_options": self.get_new_pipeline_options,
            "get_run_transforms": self.get_run_transforms,
            "get_original_pipeline": self.get_original_pipeline,
            "get_original_pipeline_options": self.get_original_pipeline_options,
            "handle_klio": klio.transforms.decorators.handle_klio,
            "show_info": self.show_info,
            # modules
            "apache_beam": beam,
            "beam": beam,
            "klio": klio,
        }

    def _info_args(self):
        return  {
            "klio_cli_version": self.job_console_config.klio_cli_version,
            "klio_version": klio_version,
            "klio_core_version": klio_core_version,
            "klio_exec_version": klio_exec_version,
            "beam_version": beam_version,
            "python_version": PY_VERSION,
            "docker_version": self.job_console_config.docker_version,
            "job_name": self.job_name,
            "config_path": self.job_console_config.config_file,
            "image_name": self.job_console_config.image_name,
        }
    
    def get_header_with_logo(self):
        return ic.REPL_BANNER.format(**self._info_args())

    def show_info(self):
        print(ic.SHOW_INFO_BANNER.format(**self._info_args()))

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

import collections
import imp
import os
import sys

import apache_beam as beam
from apache_beam import __version__ as beam_version
from apache_beam.options import pipeline_options
from notebook import notebookapp

from klio import __version__ as klio_version
from klio.transforms import core
from klio_core import __version__ as klio_core_version

from klio_exec import __version__ as klio_exec_version
from klio_exec.commands import run
from klio_exec.commands.utils import interactive_common as ic

try:
    import readline
except ImportError:
    print("Module readline not available.")
else:
    import rlcompleter
    readline.parse_and_bind("tab: complete")


_VER_INF = sys.version_info
PY_VERSION = f"{_VER_INF[0]}.{_VER_INF[1]}.{_VER_INF[2]}"

# TODO: fixme - can't import this from `klio_exec.cli` because of circular 
# import but need it to initialize `KlioPipeline` object
RuntimeConfig = collections.namedtuple(
    "RuntimeConfig", ["image_tag", "direct_runner", "update", "blocking"]
)


class InteractiveKlioContext(object):
    def __init__(self):
        self._kpipeline = None
        self.__job_console_config = None
        self.__runtime_config = None
        self.__klio_context = None

    def _get_runtime_config(self):
        image_tag = os.getenv("IMAGE_TAG")
        return RuntimeConfig(
            image_tag=image_tag,
            # default to direct and force user to explicitly choose 
            # a different runner
            direct_runner=True,
            update=None,  # not used but needs to be set
            blocking=None,  # not used but needs to be set
        )
    
    @property
    def _runtime_config(self):
        if self.__runtime_config is not None:
            return self.__runtime_config
        image_tag = os.getenv("IMAGE_TAG")
        self.__runtime_config = RuntimeConfig(
            image_tag=image_tag,
            # default to direct and force user to explicitly choose 
            # a different runner
            direct_runner=True,
            update=None,  # not used but needs to be set
            blocking=None,  # not used but needs to be set
        )
        return self.__runtime_config

    @property
    def _job_console_config(self):
        if self.__job_console_config is not None:
            return self.__job_console_config

        self.__job_console_config = dict(
            image_name=os.getenv("IMAGE_NAME"),
            docker_version=os.getenv("DOCKER_VERSION"),
            klio_cli_version=os.getenv("KLIO_CLI_VERSION"),
            config_file=os.getenv("ACTIVE_CONFIG_FILE"),
            direct_runner=True,
            notebook=None,  # not used but needs to be set
        )
        return self.__job_console_config

    @property
    def _klio_context(self):
        if self.__klio_context is not None:
            return self.__klio_context
        self.__klio_context = core.KlioContext()
        return self.__klio_context

    def _lazy_init_klio_pipeline(self):
        # only instantiate when needed; for the Python/IPython REPLs
        # this is the print out the initial banner when starting the REPL;
        # for notebooks, this is when the user executes `%klioify`
        # if self._kpipeline is not None:
            # return self._kpipeline

        config = self._klio_context.config
        # runtime_conf = self._get_runtime_config()
        self._kpipeline = run.KlioPipeline(
            config.job_name, config, self._runtime_config
        )
        return self._kpipeline

    def _get_original_pipeline(self, runner=None):
        """TODO: docstrings"""
        kpipeline = self._lazy_init_klio_pipeline()
        options = kpipeline._get_pipeline_options()
        if runner is None:
            runner = "DirectRunner"
        options.view_as(pipeline_options.StandardOptions).runner = runner
        pipeline = beam.Pipeline(options=options)
        # this adds all the default klio transforms
        kpipeline._setup_pipeline(pipeline)
        return pipeline

    def _get_original_pipeline_options(self):
        """TODO: docstrings"""
        return self._kpipeline._get_pipeline_options()

    def _get_new_pipeline(self, runner=None, options=None):
        """TODO: docstrings"""
        if options is None:
            options = self._kpipeline._get_pipeline_options()
        if runner is None:
            runner = "DirectRunner"
        options.view_as(pipeline_options.StandardOptions).runner = runner
        return beam.Pipeline(options=options)

    def _get_new_pipeline_options(self, config_dict=None):
        """TODO: docstrings"""
        if config_dict is None:
            config_dict = {}
        # TODO: check to see if there's pipeline_options already in keys
        return pipeline_options.PipelineOptions().from_dictionary(config_dict)

    def _get_version_ctx_args(self):
        return {
            "klio_cli_version": self._job_console_config["klio_cli_version"],
            "klio_version": klio_version,
            "klio_core_version": klio_core_version,
            "klio_exec_version": klio_exec_version,
            "beam_version": beam_version,
            "python_version": PY_VERSION,
            "docker_version": self._job_console_config["docker_version"],
            "job_name": self._klio_context.config.job_name,
            "config_path": self._job_console_config["config_file"],
            "image_name": self._job_console_config["image_name"],
        }
    
    def _show_info(self):
        """Print current job context and information about REPL environment."""
        print(ic.SHOW_INFO_BANNER.format(**self._get_version_ctx_args()))

    def get_local_scope_objects(self):
        import klio
        # NOTE: When adding/updating to the scope, be sure to add it to the 
        # relevant _SCOPE_* constants in `interactive_common.py` so that 
        # it's described when the console loads for user
        return {
            # variables
            "kpipeline": self._get_original_pipeline(),
            "kctx": self._klio_context,
            # functions
            "get_new_pipeline": self._get_new_pipeline,
            "get_new_pipeline_options": self._get_new_pipeline_options,
            "get_original_pipeline": self._get_original_pipeline,
            "get_original_pipeline_options": self._get_original_pipeline_options,
            "handle_klio": klio.transforms.decorators.handle_klio,
            "show_info": self._show_info,
            # modules
            "apache_beam": beam,
            "beam": beam,
            "klio": klio,
            "transforms": imp.load_source("transforms", "./transforms.py"),
        }

    def get_banner(self):
        return ic.REPL_BANNER.format(**self._get_version_ctx_args())

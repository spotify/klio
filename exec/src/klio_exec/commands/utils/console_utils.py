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

# TODO: fixme
try:
    import readline
except ImportError:
    print("Module readline not available.")
else:
    import rlcompleter
    readline.parse_and_bind("tab: complete")


DEFAULT_COLOR = "\u001b[32m"  # green
DEFAULT_COLOR_BOLD = "\u001b[32;1m"
FORMAT_RESET = "\u001b[0m"
# NOTE: PS1 and PS2 should be the same char length, or continued lines 
# won't line up correctly
PS1 = DEFAULT_COLOR + "k>> " + FORMAT_RESET
"""Custom prompt for REPL; a green 'k> ' instead of '>>> '."""
PS2 = DEFAULT_COLOR + "... " + FORMAT_RESET
"""Custom prompt for REPL; a green '... '."""

BOLD = "\u001b[1m"
CODE_COMMENT = "\u001b[38;5;251;2;3m"
# for reference
LOGO_RGB1 = (55, 120, 246)
LOGO_RGB2 = (75, 135, 247)
LOGO_RGB3 = (117, 152, 248)
LOGO_RGB4 = (158, 167, 250)
LOGO_RGB5 = (213, 187, 250)
# set foreground color with "\u0001b[38"
# the number after [38 refers to the style; 0 == normal
# the color then comes after the style - it's a rgb tuple separated by ;
# the `m` is the terminal char 
LOGO_COLOR1 = "\u001b[38;2;55;120;245;1m"
LOGO_COLOR2 = "\u001b[38;2;75;135;247;1m"
LOGO_COLOR3 = "\u001b[38;2;117;152;248;1m"
LOGO_COLOR4 = "\u001b[38;2;158;167;250;1m"
LOGO_COLOR5 = "\u001b[38;2;213;187;250;1m"
WELCOME_SPACER = " " * 16
# WELCOME_MSG = DEFAULT_COLOR_BOLD + WELCOME_SPACER + "Welcome to the Klio console!" + FORMAT_RESET
WELCOME_MSG = DEFAULT_COLOR_BOLD + "Welcome to the Klio console!" + FORMAT_RESET
# NOTE: This variable is 4 chars long, so that plus `{}`  in the string 
# replacement below equals the same length as the actual string
COL1 = LOGO_COLOR1 + "******" + FORMAT_RESET
COL2 = LOGO_COLOR2 + "******" + FORMAT_RESET
COL3 = LOGO_COLOR3 + "******" + FORMAT_RESET
COL4 = LOGO_COLOR4 + "******" + FORMAT_RESET
COL5 = LOGO_COLOR5 + "******" + FORMAT_RESET

HLOGO = rf"""
                                      {COL4}             
                                      {COL4}             
                                      {COL4}             
{COL1}                    {COL3}                   {COL5}
{COL1}                    {COL3}      {COL4}       {COL5}
{COL1}                    {COL3}      {COL4}       {COL5}
                                      {COL4}             
{COL1}                    {COL3}                   {COL5}
{COL1}                    {COL3}                   {COL5}
{COL1}                    {COL3}                   {COL5}
             {COL2}                                        
{COL1}       {COL2}       {COL3}                   {COL5}
{COL1}       {COL2}       {COL3}                   {COL5}
{COL1}                    {COL3}                   {COL5}
             {COL2}                                        
             {COL2}                                        
             {COL2}                                        
"""

HINTRO = f"""{HLOGO}
{WELCOME_MSG}

"""

LOGO_COLOR3_BOLD = "\u001b[38;2;117;152;248;1m"
LOGO_COLOR3_BOLD_UNDERLINE = "\u001b[38;2;117;152;248;1;4m"

HVERSIONS_INTRO = (
    f"{LOGO_COLOR3_BOLD}Running with the following versions:{FORMAT_RESET}"
)
HVERSIONS = """
    klio-cli    : {klio_cli_version}
    klio        : {klio_version} (inside job Docker image)
    klio-core   : {klio_core_version} (inside job Docker image)
    klio-exec   : {klio_exec_version} (inside job Docker image)
    apache-beam : {beam_version} (inside job Docker image)
    Docker      : {docker_version}
    Image       : {image_name}

"""
HEADER_CTX_LINE = f"{LOGO_COLOR3_BOLD}Current Context:{FORMAT_RESET}"
HEADER_CTX = """
    Job Name    : {job_name}
    Config File : {config_path}
"""

HEADER_SCOPE = f"""
{LOGO_COLOR3_BOLD}Available variables in scope (call {FORMAT_RESET}{LOGO_COLOR5}`help`{FORMAT_RESET}{LOGO_COLOR3_BOLD} on any variable to get more information):{FORMAT_RESET}
    kpipeline : the Beam pipeline that Klio constructs with your `run.py`
    kctx      : the KlioContext containing configuration, logger, metrics, etc.

{LOGO_COLOR3_BOLD}Available functions in scope (call {FORMAT_RESET}{LOGO_COLOR5}`help`{FORMAT_RESET}{LOGO_COLOR3_BOLD} on any function to get more information):{FORMAT_RESET}
    get_new_pipeline              : create a new Beam Pipeline
    get_new_pipeline_options      : create new Beam pipeline options object
    get_original_pipeline         : get original pipeline Klio constructed from `run.py`
    get_original_pipeline_options : get original pipeline options used to construct `kpipeline`
    handle_klio                   : transform decorator for handling Klio messages

{LOGO_COLOR3_BOLD}Available modules in scope (call {FORMAT_RESET}{LOGO_COLOR5}`help`{FORMAT_RESET}{LOGO_COLOR3_BOLD} on any module to get more information):{FORMAT_RESET}
    apache_beam : the top-level Apache Beam module as installed in the job's container
    beam        : alias to `apache_beam` module
    klio        : the `klio` library

{LOGO_COLOR3_BOLD}Examples:{FORMAT_RESET}
    {CODE_COMMENT}# To run the `kpipeline` with DirectRunner (default):{FORMAT_RESET}
    {LOGO_COLOR5}k>> {FORMAT_RESET}result = kpipeline.run()
    {CODE_COMMENT}# To run the `kpipeline` on Dataflow:{FORMAT_RESET}
    {LOGO_COLOR5}k>> {FORMAT_RESET}pipeline = get_original_pipeline(runner="DataflowRunner")
    {LOGO_COLOR5}k>> {FORMAT_RESET}result = pipeline.run()
    {CODE_COMMENT}# To create & run a new pipeline from scratch:{FORMAT_RESET}
    {LOGO_COLOR5}k>> {FORMAT_RESET}p = get_new_pipeline(runner="DirectRunner")
    {LOGO_COLOR5}k>> {FORMAT_RESET}output = p | beam.Create(["foo", "bar"]) | beam.Map(print)
    {LOGO_COLOR5}k>> {FORMAT_RESET}result = p.run()
    foo
    bar
    {CODE_COMMENT}# Another example with the handle_klio decorator:{FORMAT_RESET}
    {LOGO_COLOR5}k>> {FORMAT_RESET}@handle_klio
    {LOGO_COLOR5}... {FORMAT_RESET}def new_transform(ctx, item):
    {LOGO_COLOR5}... {FORMAT_RESET}  element = item.element.decode("utf-8")
    {LOGO_COLOR5}... {FORMAT_RESET}  print("Received %s" % element)
    {LOGO_COLOR5}... {FORMAT_RESET}  return item
    {LOGO_COLOR5}... {FORMAT_RESET}
    {LOGO_COLOR5}k>> {FORMAT_RESET}p = get_new_pipeline()  # defaults to DirectRunner
    {LOGO_COLOR5}k>> {FORMAT_RESET}ids = p | klio.transforms.KlioReadFromText("entity_ids.txt") 
    {LOGO_COLOR5}k>> {FORMAT_RESET}out = ids | beam.Map(new_transform)
    {LOGO_COLOR5}k>> {FORMAT_RESET}result = p.run()
    Received entity_id1
    Received entity_id2
    Received entity_id3
    Received entity_id4

Call `show_header()` to see this intro again. 
Call `exit()` or use "CTRL+D" to exit.
"""
HEADER_TMPL = (
    f"{HVERSIONS_INTRO}{HVERSIONS}{HEADER_CTX_LINE}"
    f"{HEADER_CTX}{HEADER_SCOPE}"
)
HEADER_TMPL_LOGO = f"{HINTRO}{HEADER_TMPL}"
EXIT_MSG = "Exiting the Klio console..."

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

    def get_graph(self):
        graph = pipeline_graph.PipelineGraph(self)
        dot_graph = graph._get_graph()
        g = nx_pydot.from_pydot(dot_graph)

    def __repr__(self):
        # TODO: what to name this? prob not with `id` - maybe something more recognizable
        return f"<KlioInteractiveBeamPipeline(id={id(self)})>"


class KlioConsoleContextManager(run.KlioPipeline):
    """TODO: docstrings"""

    def __init__(self, *args, job_console_config=None, **kwargs):
        super(KlioConsoleContextManager, self).__init__(*args, **kwargs)
        self.job_console_config = job_console_config

    @staticmethod
    def __parse_runner(runner):
        if runner is None:
            runner = "DirectRunner"

        elif runner.lower() not in ["dataflowrunner", "directrunner", "dataflow", "direct"]:
            raise Exception(f"Unrecognized runner: {runner}")

        else:
            runner = {
                "dataflowrunner": "DataflowRunner",
                "dataflow": "DataflowRunner",
                "directrunner": "DirectRunner",
                "direct": "DirectRunner",
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
        # HEADER_SCOPE above so it's described when the console loads for user
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
            "show_header": self.show_header,
            # modules
            "apache_beam": beam,
            "beam": beam,
            "klio": klio,
        }
    
    def get_header_with_logo(self):
        return HEADER_TMPL_LOGO.format(
            klio_cli_version=self.job_console_config.klio_cli_version,
            docker_version=self.job_console_config.docker_version,
            image_name=self.job_console_config.image_name,
            klio_version=klio_version,
            klio_core_version=klio_core_version,
            klio_exec_version=klio_exec_version,
            config_path=self.job_console_config.config_file,
            beam_version=beam_version,
            job_name=self.job_name,
        )

    def show_header(self):
        print(HEADER_TMPL.format(
            klio_cli_version=self.job_console_config.klio_cli_version,
            docker_version=self.job_console_config.docker_version,
            image_name=self.job_console_config.image_name,
            klio_version=klio_version,
            klio_core_version=klio_core_version,
            klio_exec_version=klio_exec_version,
            config_path=self.job_console_config.config_file,
            beam_version=beam_version,
            job_name=self.job_name,
        ))

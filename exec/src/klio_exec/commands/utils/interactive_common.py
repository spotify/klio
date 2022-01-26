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

import colored


# --- Styles ---
BOLD = colored.attr("bold")
UNDERLINE = colored.attr("underlined")
ITALIC = "\x1b[3m"  # colored doesn't support italics, but the terminal will

# --- Colors ---
# ANSI color code <-> rgb <-> HEX <-> XTerm name: 
# https://jonasjacek.github.io/colors/
GREY = colored.fg(249)  # grey70 #b2b2b2
GREEN = colored.fg(42)  # SpringGreen2 #00d787
DEFAULT_COLOR = GREEN

# Logo color palette: https://coolors.co/3778f6-4b87f7-7598f8-9ea7fa-d5bbfa
# Using HEX codes because these don't map to ANSI color codes
LOGO_CLR_1 = colored.fg("#3778F6")  # Blue Crayola
LOGO_CLR_2 = colored.fg("#4b87f7")  # United Nations Blue
LOGO_CLR_3 = colored.fg("#7598f8")  # Cornflower Blue
LOGO_CLR_4 = colored.fg("#9ea7fa")  # Maximum Blue Purple
LOGO_CLR_5 = colored.fg("#d5bbfa")  # Mauve


# --- General ---
PROMPT1 = "k>> "
PROMPT2 = "... "
# NOTE: REPL_PROMPT1 and REPL_PROMPT2 should be the same char length, 
# or continued lines won't line up correctly when in the REPL itself
REPL_PROMPT1 = colored.stylize(PROMPT1, DEFAULT_COLOR)
"""Custom prompt for REPL; a green 'k>> ' instead of '>>> '."""
REPL_PROMPT2 = colored.stylize(PROMPT2, DEFAULT_COLOR)
"""Custom prompt for REPL; a green '... '."""

# --- ASCII Logo ---

LINE = "*" * 6
COL1 = colored.stylize(LINE, LOGO_CLR_1)
COL2 = colored.stylize(LINE, LOGO_CLR_2)
COL3 = colored.stylize(LINE, LOGO_CLR_3)
COL4 = colored.stylize(LINE, LOGO_CLR_4)
COL5 = colored.stylize(LINE, LOGO_CLR_5)

LOGO = rf"""
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

# --- Headers/Banner ---
WELCOME_MSG = "Welcome to the Klio console!"
_WELCOME_MSG_STYLIZED = colored.stylize(WELCOME_MSG, DEFAULT_COLOR + BOLD)

# used for the plain & IPython REPL (not notebook)
REPL_INTRO = f"""{LOGO}
{_WELCOME_MSG_STYLIZED}"""

# Version headers
_VERSION_INTRO = "Running with the following versions:"
_VERSION_INTRO = colored.stylize(_VERSION_INTRO, LOGO_CLR_3 + BOLD)
_VERSION_INFO_TMPL = """
    klio-cli    : {klio_cli_version}
    klio        : {klio_version} (inside job's Docker image)
    klio-core   : {klio_core_version} (inside job's Docker image)
    klio-exec   : {klio_exec_version} (inside job's Docker image)
    apache-beam : {beam_version} (inside job's Docker image)
    Python      : {python_version}  (inside job's Docker image)
    Docker      : {docker_version}
"""
VERSION_HEADER = f"{_VERSION_INTRO}{_VERSION_INFO_TMPL}"

# Job context headers
_CTX_INTRO = "Current context:"
_CTX_INTRO = colored.stylize(_CTX_INTRO, LOGO_CLR_3 + BOLD)
_CTX_INFO_TMPL = """
    Job Name    : {job_name}
    Config File : {config_path}
    Image       : {image_name}"""
CTX_HEADER = f"{_CTX_INTRO}{_CTX_INFO_TMPL}"

# Available scope headers
# -- Available scope - common

# "(call `help` on any (variable|function|module) to get more information):"
_SCOPE_HELP_PFX = colored.stylize("(call ", LOGO_CLR_3 + BOLD)
_SCOPE_HELP_FUNC = colored.stylize("`help`", LOGO_CLR_5 + BOLD)
_SCOPE_HELP_SFX = colored.stylize(" on any {scope_obj} to get more information):", LOGO_CLR_3 + BOLD)
_SCOPE_HELP = f"{_SCOPE_HELP_PFX}{_SCOPE_HELP_FUNC}{_SCOPE_HELP_SFX}"

# -- Available scope - variables
_SCOPE_VAR_INTRO = "Available variables in scope "
_SCOPE_VAR_INTRO_STYLIZED = colored.stylize(_SCOPE_VAR_INTRO, LOGO_CLR_3 + BOLD)
_SCOPE_VAR_LINE = f"{_SCOPE_VAR_INTRO_STYLIZED}{_SCOPE_HELP.format(scope_obj='variable')}"
_SCOPE_VAR_INFO = f"""
{_SCOPE_VAR_LINE}
    kpipeline : the Beam pipeline that Klio constructs with your `run.py`
    kctx      : the KlioContext containing configuration, logger, metrics, etc.
"""

# -- Available scope - functions
_SCOPE_FUNC_INTRO = "Available functions in scope "
_SCOPE_FUNC_INTRO_STYLIZED = colored.stylize(_SCOPE_FUNC_INTRO, LOGO_CLR_3 + BOLD)
_SCOPE_FUNC_LINE = f"{_SCOPE_FUNC_INTRO_STYLIZED}{_SCOPE_HELP.format(scope_obj='function')}"
_SCOPE_FUNC_INFO = f"""
{_SCOPE_FUNC_LINE}
    get_new_pipeline              : create a new Beam Pipeline
    get_new_pipeline_options      : create new Beam pipeline options object
    get_original_pipeline         : get original pipeline Klio constructed from `run.py`
    get_original_pipeline_options : get original pipeline options used to construct `kpipeline`
    handle_klio                   : transform decorator for handling Klio messages
"""

# -- Available scope - modules
_SCOPE_MOD_INTRO = "Available modules in scope "
_SCOPE_MOD_INTRO_STYLIZED = colored.stylize(_SCOPE_MOD_INTRO, LOGO_CLR_3 + BOLD)
_SCOPE_MOD_LINE = f"{_SCOPE_MOD_INTRO_STYLIZED}{_SCOPE_HELP.format(scope_obj='module')}"
_SCOPE_MOD_INFO = f"""
{_SCOPE_MOD_LINE}
    apache_beam : the top-level Apache Beam module as installed in the job's container
    beam        : alias to `apache_beam` module
    klio        : the `klio` library
"""

SCOPE_HEADER=f"{_SCOPE_VAR_INFO}{_SCOPE_FUNC_INFO}{_SCOPE_MOD_INFO}"

# Example headers
_EX_INDENT = r"    "
_EX_PROMPT1_STYLIZED = colored.stylize(PROMPT1, LOGO_CLR_5)
_EX_PROMPT2_STYLIZED = colored.stylize(PROMPT2, LOGO_CLR_5)
_EX_PROMPT1 = f"{_EX_INDENT}{_EX_PROMPT1_STYLIZED}"
_EX_PROMPT2 = f"{_EX_INDENT}{_EX_PROMPT2_STYLIZED}"
COMMENT = GREY + ITALIC

_EX_INTRO = "Examples:"
_EX_INTRO = colored.stylize(_EX_INTRO, LOGO_CLR_3 + BOLD)

# -- Example 1
_EX1_COMMENT = f"{_EX_INDENT}# To run the `kpipeline` with DirectRunner (default):"
_EX1_COMMENT = colored.stylize(_EX1_COMMENT, COMMENT)
_EX1_CODE = f"{_EX_PROMPT1}result = kpipeline.run()"
_EX1 = f"""{_EX1_COMMENT}
{_EX1_CODE}"""

# -- Example 2
_EX2_COMMENT = f"{_EX_INDENT}# To run the `kpipeline` on Dataflow:"
_EX2_COMMENT = colored.stylize(_EX2_COMMENT, COMMENT)
_EX2_CODE_LN1 = "pipeline = get_original_pipeline(runner='DataflowRunner')"
_EX2_CODE_LN1 = f"{_EX_PROMPT1}{_EX2_CODE_LN1}"
_EX2_CODE_LN2 = f"{_EX_PROMPT1}result = pipeline.run()"
_EX2 = f"""{_EX2_COMMENT}
{_EX2_CODE_LN1}
{_EX2_CODE_LN2}"""

# -- Example 3
_EX3_COMMENT = f"{_EX_INDENT}# To create & run a new pipeline from scratch:"
_EX3_COMMENT = colored.stylize(_EX3_COMMENT, COMMENT)
_EX3_CODE_LN1 = f"{_EX_PROMPT1}p = get_new_pipeline(runner='DirectRunner')"
_EX3_CODE_LN2 = "output = p | beam.Create(['foo', 'bar']) | beam.Map(print)"
_EX3_CODE_LN2 = f"{_EX_PROMPT1}{_EX3_CODE_LN2}"
_EX3_CODE_LN3 = f"{_EX_PROMPT1}result = p.run()"
_EX3_OUTPUT = f"{_EX_INDENT}foo\n{_EX_INDENT}bar"
_EX3 = f"""{_EX3_COMMENT}
{_EX3_CODE_LN1}
{_EX3_CODE_LN2}
{_EX3_CODE_LN3}
{_EX3_OUTPUT}"""

# -- Example 4
_EX4_COMMENT = f"{_EX_INDENT}# Another example with the handle_klio decorator:"
_EX4_COMMENT = colored.stylize(_EX4_COMMENT, COMMENT)
_EX4_CODE_LN1 = f"{_EX_PROMPT1}@handle_klio"
_EX4_CODE_LN2 = f"{_EX_PROMPT2}def new_transform(ctx, item):"
_EX4_CODE_LN3 = f"{_EX_PROMPT2}  element = item.element.decode('utf-8')"
_EX4_CODE_LN4 = f"{_EX_PROMPT2}  print('Received %s' % element)"
_EX4_CODE_LN5 = f"{_EX_PROMPT2}  return item"
_EX4_CODE_LN6 = f"{_EX_PROMPT2}"
_EX4_CODE_LN7_CMT = colored.stylize("  # defaults to DirectRunner", COMMENT)
_EX4_CODE_LN7 = f"{_EX_PROMPT1}p = get_new_pipeline(){_EX4_CODE_LN7_CMT}"
_EX4_CODE_LN8 = "ids = p | klio.transforms.KlioReadFromText('entity_ids.txt')"
_EX4_CODE_LN8 = f"{_EX_PROMPT1}{_EX4_CODE_LN8}"
_EX4_CODE_LN9 = f"{_EX_PROMPT1}out = ids | beam.Map(new_transform)"
_EX4_CODE_LN10 = f"{_EX_PROMPT1}result = p.run()"
_EX4_OUTPUT = f"""{_EX_INDENT}Received entity_id1
{_EX_INDENT}Received entity_id2
{_EX_INDENT}Received entity_id3
{_EX_INDENT}Received entity_id4
"""
_EX4 = f"""{_EX4_COMMENT}
{_EX4_CODE_LN1}
{_EX4_CODE_LN2}
{_EX4_CODE_LN3}
{_EX4_CODE_LN4}
{_EX4_CODE_LN5}
{_EX4_CODE_LN6}
{_EX4_CODE_LN7}
{_EX4_CODE_LN8}
{_EX4_CODE_LN9}
{_EX4_CODE_LN10}
{_EX4_OUTPUT}"""

EXAMPLES_HEADER = f"""{_EX_INTRO}
{_EX1}
{_EX2}
{_EX3}
{_EX4}"""

# --- Suffixes ---
REPL_SUFFIX = """Call `show_info()` to see this information again.
Call `exit()` or use "CTRL+D" to exit.
"""

# --- Full banners ---
SHOW_INFO_BANNER = f"""{VERSION_HEADER}
{CTX_HEADER}
{SCOPE_HEADER}
{EXAMPLES_HEADER}
{REPL_SUFFIX}"""

REPL_BANNER = f"""{REPL_INTRO}

{SHOW_INFO_BANNER}"""

# --- Other ---
EXIT_MSG = "Exiting the Klio console..."
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

import click


class MutuallyExclusiveOption(click.Option):
    """
    Helper class to validate and document mutual exclusivity between options.

    This unfortunately doesn't work with click arguments (only options).

    To use, pass in both of the following keywords into an option declaration:
        click.option(
            "--an-option"
            cls=MutuallyExclusiveOption
            mutually_exclusive=["string_of_exclusive_option", "another_option"]
        )
    """

    def __init__(self, *args, **kwargs):
        self.mut_ex_opts = set(kwargs.pop("mutually_exclusive", []))
        help_text = kwargs.get("help", "")
        if self.mut_ex_opts:
            mutex = [self._varname_to_opt_flag(m) for m in self.mut_ex_opts]
            mutex_fmted = ["``{}``".format(m) for m in mutex]
            ex_str = ", ".join(mutex_fmted)
            kwargs["help"] = help_text + (
                "\n\n**NOTE:** This option is mutually exclusive with "
                "[{}].".format(ex_str)
            )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    @staticmethod
    def _varname_to_opt_flag(var):
        return "--" + var.replace("_", "-")

    def handle_parse_result(self, ctx, opts, args):
        if self.mut_ex_opts.intersection(opts) and self.name in opts:
            mutex = [
                "`" + self._varname_to_opt_flag(m) + "`"
                for m in self.mut_ex_opts
            ]
            mutex = ", ".join(mutex)
            msg = "Illegal usage: `{}` is mutually exclusive with {}.".format(
                self._varname_to_opt_flag(self.name), mutex
            )
            raise click.UsageError(msg)

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx, opts, args
        )


def image_tag(*args, **kwargs):
    """Docker image tag to use."""

    def wrapper(func):
        return click.option(
            "--image-tag", help="Docker image tag to use", **kwargs
        )(func)

    if args:
        return wrapper(args[0])
    return wrapper


#####
# common options
#####
def direct_runner(func):
    """Run the job locally via the DirectRunner."""
    return click.option(
        "--direct-runner",
        default=False,
        is_flag=True,
        help="Run the job locally via the DirectRunner.",
    )(func)


def update(func):
    """[Experimental] Update an existing streaming Cloud Dataflow job."""
    return click.option(
        "--update/--no-update",
        default=None,
        is_flag=True,
        help="[Experimental] Update an existing streaming Cloud Dataflow job.",
    )(func)


def show_logs(func):
    """Show a job's logs while profiling."""
    return click.option(
        "--show-logs",
        default=False,
        show_default=True,
        is_flag=True,
        help="Show a job's logs while profiling.",
    )(func)


#####
# options for `<cmd> profile memory`
#####
def interval(func):
    """Sampling period (in seconds)."""
    return click.option(
        "--interval",
        default=0.1,
        show_default=True,
        type=float,
        help="Sampling period (in seconds).",
    )(func)


def include_children(func):
    """Monitor forked processes as well (sums up all process memory)."""
    return click.option(
        "--include-children",
        default=False,
        show_default=True,
        is_flag=True,
        help="Monitor forked processes as well (sums up all process memory).",
    )(func)


def multiprocess(func):
    """Monitor forked processes creating individual plots for each child."""
    return click.option(
        "--multiprocess",
        default=False,
        show_default=True,
        is_flag=True,
        help=(
            "Monitor forked processes creating individual plots for each "
            "child."
        ),
    )(func)


def plot_graph(func):
    """
    Plot memory profile using matplotlib.
    Saves to klio_profile_memory_<YYYYMMDDhhmmss>.png
    """
    return click.option(
        "-g",
        "--plot-graph",
        default=False,
        show_default=True,
        is_flag=True,
        help=(
            "Plot memory profile using matplotlib. Saves to "
            "klio_profile_memory_<YYYYMMDDhhmmss>.png."
        ),
    )(func)


#####
# options for `<cmd> profile memory-per-line`
#####
def maximum(func):
    """
    Print maximum memory usage per line in aggregate of all input
    elements processed
    """
    return click.option(
        "--maximum",
        "get_maximum",
        default=False,
        show_default=True,
        is_flag=True,
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["per_element"],
        help=(
            "Print maximum memory usage per line in aggregate of all input "
            "elements process."
        ),
    )(func)


def per_element(*args, **kwargs):
    """Print memory usage per line for each input element processed"""

    def wrapper(func):
        return click.option(
            "--per-element",
            default=False,
            is_flag=True,
            cls=MutuallyExclusiveOption,
            mutually_exclusive=["maximum"],
            help="Print memory usage per line for each input element processed",
            **kwargs  # added (and no more show_default keyword arg)
        )(func)

    if args:
        return wrapper(args[0])
    return wrapper


#####
# options for `<cmd> profile timeit`
#####
def iterations(func):
    """Number of times to execute each entity ID provided."""
    return click.option(
        "-n",
        "--iterations",
        default=10,
        show_default=True,
        type=int,
        help="Number of times to execute each entity ID provided.",
    )(func)


################################
# options for config overrides
################################


def override(func):
    """Override a config value, in the form ``key=value``."""
    return click.option(
        "-O",
        "--override",
        default=[],
        multiple=True,
        help="Override a config value, in the form ``key=value``.",
    )(func)


def template(func):
    """
    Set the value of a config template parameter in the form ``key=value``.
    Any instance of ``${key}`` in ``klio-job.yaml`` will be replaced
    with ``value``.
    """
    return click.option(
        "-T",
        "--template",
        default=[],
        multiple=True,
        help=(
            "Set the value of a config template parameter"
            ", in the form ``key=value``.  Any instance of ``${key}`` "
            "in ``klio-job.yaml`` will be replaced with ``value``."
        ),
    )(func)


def config_file(*args, **kwargs):
    """Path to config filename. If ``PATH`` is not absolute,
    it will be treated relative to ``--job-dir``.
    Defaults to ``klio-job.yaml``.
    """
    mutually_exclusive = kwargs.get("mutex", [])

    def wrapper(func):
        return click.option(
            "-c",
            "--config-file",
            type=click.Path(exists=False),
            help=(
                "Path to config filename. If ``PATH`` is not absolute, it "
                "will be treated relative to ``--job-dir``. Defaults to "
                "``klio-job.yaml``."
            ),
            cls=MutuallyExclusiveOption,
            mutually_exclusive=mutually_exclusive,
        )(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


def job_dir(*args, **kwargs):
    """
    Job directory where the job's ``Dockerfile`` is located.
    Defaults current working directory.
    """
    mutually_exclusive = kwargs.get("mutex", [])

    def wrapper(func):
        return click.option(
            "-j",
            "--job-dir",
            type=click.Path(exists=True),
            help=(
                "Job directory where the job's ``Dockerfile`` is located. "
                "Defaults current working directory."
            ),
            cls=MutuallyExclusiveOption,
            mutually_exclusive=mutually_exclusive,
        )(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper

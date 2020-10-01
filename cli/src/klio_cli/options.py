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


#####
# utils for options
#####
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


def _verify_gcs_uri(ctx, param, value):
    if value and not value.startswith("gs://"):
        raise click.BadParameter(
            "Unsupported location type. Please provide a GCS location with "
            "the `gs://` prefix."
        )

    return value


#####
# common options
#####
def job_dir(*args, **kwargs):
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


def config_file(*args, **kwargs):
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


def override(func):
    return click.option(
        "-O",
        "--override",
        default=[],
        multiple=True,
        help="Override a config value, in the form ``key=value``.",
    )(func)


def template(func):
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


def image_tag(func):
    return click.option(
        "--image-tag",
        default=None,
        show_default="``git-sha[dirty?]``",
        help="Docker image tag to use.",
    )(func)


def force_build(func):
    return click.option(
        "--force-build",
        default=False,
        is_flag=True,
        help="Build Docker image even if you already have it.",
    )(func)


def direct_runner(func):
    return click.option(
        "--direct-runner",
        default=False,
        is_flag=True,
        help="Run the job locally via the DirectRunner.",
    )(func)


def update(func):
    return click.option(
        "--update/--no-update",
        default=None,
        is_flag=True,
        help="[Experimental] Update an existing streaming Cloud Dataflow job.",
    )(func)


def runtime(func):
    func = force_build(func)
    func = direct_runner(func)
    func = update(func)
    return func


def job_name(*args, **kwargs):
    def wrapper(func):
        return click.option("--job-name", **kwargs)(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


def region(func):
    return click.option(
        "--region",
        help=(
            "Region of job, if neither ``--job-dir`` nor ``--config-file`` is "
            "not provided."
        ),
    )(func)


def gcp_project(*args, **kwargs):
    def wrapper(func):
        return click.option("--gcp-project", **kwargs)(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


#####
# options for `klio job verify
#####
def create_resources(func):
    return click.option(
        "--create-resources",
        default=False,
        is_flag=True,
        help=(
            "Create missing GCP resources based on ``klio-info.yaml``."
            " Default: ``False``"
        ),
    )(func)


#####
# options for `klio job audit`
#####
def list_steps(func):
    return click.option(
        "--list",
        "list_steps",
        is_flag=True,
        default=False,
        help="List available audit steps (does not run any audits).",
    )(func)


#####
# options for `klio job create`
#####
def output(func):
    return click.option(
        "--output",
        help="Output directory. Defaults to current working directory.",
    )(func)


def use_defaults(func):
    return click.option(
        "--use-defaults",
        default=False,
        is_flag=True,
        help="Accept default values.",
    )(func)


#####
# common options for `klio job profile <subcommand>`
#####
def input_file(*args, **kwargs):
    mutually_exclusive = kwargs.pop("mutex", [])

    def wrapper(func):
        return click.option(
            "-i",
            "--input-file",
            type=click.Path(exists=False, dir_okay=False, readable=True),
            cls=MutuallyExclusiveOption,
            mutually_exclusive=mutually_exclusive,
            **kwargs,
        )(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


def output_file(*args, **kwargs):
    mutually_exclusive = kwargs.pop("mutex", [])

    def wrapper(func):
        return click.option(
            "-o",
            "--output-file",
            type=click.Path(exists=False, dir_okay=False, writable=True),
            cls=MutuallyExclusiveOption,
            mutually_exclusive=mutually_exclusive,
            **kwargs,
        )(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


def show_logs(func):
    return click.option(
        "--show-logs",
        default=False,
        show_default=True,
        is_flag=True,
        help="Show a job's logs while profiling.",
    )(func)


#####
# options for `klio job profile collect-profiling-data`
#####
def gcs_location(func):
    return click.option(
        "--gcs-location",
        default=None,
        show_default="`pipeline_options.profile_location` in `klio-job.yaml`",
        help="GCS location of cProfile data.",
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["job_dir", "input_file", "config_file"],
        callback=_verify_gcs_uri,
    )(func)


def since(func):
    return click.option(
        "--since",
        default=None,
        show_default="1 hour ago",
        help=(
            "Start time, relative or absolute (interpreted by "
            "``dateparser.parse``). "
        ),
    )(func)


def until(func):
    return click.option(
        "--until",
        default=None,
        show_default="now",
        help=(
            "End time, relative or absolute (interpreted by "
            "``dateparser.parse``). "
        ),
    )(func)


# https://docs.python.org/3.5/library/profile.html#pstats.Stats.sort_stats
# unfortunately, can't get this dynamically <3.7 (@lynn)
SORT_STATS_KEY = [
    "calls",
    "cumulative",
    "cumtime",
    "file",
    "filename",
    "module",
    "ncalls",
    "pcalls",
    "line",
    "name",
    "nfl",
    "stdname",
    "time",
    "tottime",
]


def sort_stats(func):
    return click.option(
        "--sort-stats",
        multiple=True,
        default=["tottime"],
        show_default=True,
        type=click.Choice(SORT_STATS_KEY, case_sensitive=False),
        help=(
            "Sort output of profiling statistics as supported by "
            "`sort_stats <https://docs.python.org/3/library/profile.html#"
            "pstats.Stats.sort_stats>`_. Multiple ``--sort-stats`` invocations "
            "are supported."
        ),
    )(func)


#####
# options for `klio job profile memory`
#####
def interval(func):
    return click.option(
        "--interval",
        default=0.1,
        show_default=True,
        type=float,
        help="Sampling period (in seconds).",
    )(func)


def include_children(func):
    return click.option(
        "--include-children",
        default=False,
        show_default=True,
        is_flag=True,
        help="Monitor forked processes as well (sums up all process memory).",
    )(func)


def multiprocess(func):
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
    return click.option(
        "-g",
        "--plot-graph",
        default=False,
        show_default=True,
        is_flag=True,
        help=(
            "Plot memory profile using matplotlib. Saves to "
            "``klio_profile_memory_<YYYYMMDDhhmmss>.png``."
        ),
    )(func)


#####
# options for `klio job profile memory-per-line`
#####
def maximum(func):
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


def per_element(func):
    return click.option(
        "--per-element",
        default=False,
        show_default=True,
        is_flag=True,
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["maximum"],
        help="Print memory usage per line for each input element processed.",
    )(func)


#####
# options for `klio job profile timeit`
#####
def iterations(func):
    return click.option(
        "-n",
        "--iterations",
        default=10,
        show_default=True,
        type=int,
        help="Number of times to execute each entity ID provided.",
    )(func)


#####
# options for `klio message publish`
#####
def force(func):
    return click.option(
        "-f",
        "--force",
        default=False,
        is_flag=True,
        help="Force processing even if output data exists.",
    )(func)


def ping(func):
    return click.option(
        "-p",
        "--ping",
        default=False,
        is_flag=True,
        help="Skip the processing of an ID to trace the path the ID takes.",
    )(func)


def top_down(func):
    return click.option(
        "-t",
        "--top-down",
        default=False,
        is_flag=True,
        help=(
            "Trigger an apex node and all child nodes below. Default: "
            "``False``"
        ),
    )(func)


def bottom_up(func):
    return click.option(
        "-b",
        "--bottom-up",
        # default is actually true, but initially conflicts w top-down logic
        default=False,
        is_flag=True,
        help=(
            "Trigger work for only this job and any required parent jobs, "
            "but no sibling or child jobs. Default: ``True``"
        ),
    )(func)


def non_klio(func):
    return click.option(
        "-n",
        "--non-klio",
        default=False,
        is_flag=True,
        help=(
            "Publish a free-form, non-Klio message to the targeted job. The "
            "targeted job must also support non-Klio messages. Mutually "
            "exclusive with ``--force``, ``--ping``, and ``--bottom-up``. "
            "Default: ``False``"
        ),
    )(func)

# Copyright 2020 Spotify AB

import click


#####
# utils for options
#####
# TODO: this is the same as in cli/src/klio_cli/options.py; move to core @lynn
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
            ex_str = ", ".join(mutex)
            kwargs["help"] = help_text + (
                " NOTE: This option is mutually exclusive with [{}].".format(
                    ex_str
                )
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


#####
# options for `klioexec run`
#####
def image_tag(func):
    return click.option("--image-tag", help="Docker image tag to use")(func)


def direct_runner(func):
    return click.option(
        "--direct-runner",
        default=False,
        is_flag=True,
        help="Run the job locally via the DirectRunner",
    )(func)


def update(func):
    return click.option(
        "--update/--no-update",
        default=None,
        is_flag=True,
        help="[Experimental] Update an existing streaming Cloud Dataflow job.",
    )(func)


def config_file(func):
    return click.option(
        "-c",
        "--config-file",
        type=click.Path(exists=True, resolve_path=True, dir_okay=False),
        help=(
            "Path to config filename. Defaults to `klio-job.yaml` in the "
            "current working directory. "
        ),
    )(func)


def blocking(func):
    return click.option(
        "--blocking/--no-blocking",
        default=None,
        is_flag=True,
        help="Wait for Dataflow job to finish before returning",
    )(func)


#####
# options for `klioexec profile`
#####
def input_file(func):
    # mutually exclusive with entity ID click args
    return click.option(
        "-i",
        "--input-file",
        type=click.Path(exists=True, dir_okay=False, readable=True),
        help=(
            "File of entity IDs (separated by a new line character) with "
            "which to profile a Klio job."
        ),
        required=False,
    )(func)


def output_file(func):
    return click.option(
        "-o",
        "--output-file",
        type=click.Path(exists=False, dir_okay=False, writable=True),
        default=None,
        show_default="stdout",
        help="Output file for results.",
    )(func)


def show_logs(func):
    return click.option(
        "--show-logs",
        default=False,
        show_default=True,
        is_flag=True,
        help="Show a job's logs while profiling.",
    )(func)


#####
# options for `klioexec profile memory
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
            "klio_profile_memory_<YYYYMMDDhhmmss>.png."
        ),
    )(func)


#####
# options for `klioexec profile memory-per-line`
#####
def maximum(func):
    return click.option(
        "--maximum",  # FYI "--max" conflicts with Beam's arg parsing
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
        show_default="True",  # defaults to true in func logic
        is_flag=True,
        cls=MutuallyExclusiveOption,
        mutually_exclusive=["maximum"],
        help="Print memory usage per line for each input element processed.",
    )(func)


#####
# options for `klioexec profile timeit`
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

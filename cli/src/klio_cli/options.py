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

from klio_core import options as core_options


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


def force_build(func):
    return click.option(
        "--force-build",
        default=False,
        is_flag=True,
        help="Build Docker image even if you already have it.",
    )(func)


def runtime(func):
    func = force_build(func)
    func = core_options.direct_runner(func)
    func = core_options.update(func)
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
            cls=core_options.MutuallyExclusiveOption,
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
            cls=core_options.MutuallyExclusiveOption,
            mutually_exclusive=mutually_exclusive,
            **kwargs,
        )(func)

    # allows @options.foo to be used without parens (i.e. no need to do
    # `@options.foo()`) when there are no args/kwargs provided
    if args:
        return wrapper(args[0])
    return wrapper


#####
# options for `klio job profile collect-profiling-data`
#####
def gcs_location(func):
    return click.option(
        "--gcs-location",
        default=None,
        show_default="`pipeline_options.profile_location` in `klio-job.yaml`",
        help="GCS location of cProfile data.",
        cls=core_options.MutuallyExclusiveOption,
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


# https://docs.python.org/3.6/library/profile.html#pstats.Stats.sort_stats
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

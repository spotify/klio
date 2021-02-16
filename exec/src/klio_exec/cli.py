# Copyright 2019-2020 Spotify AB
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

import collections
import logging
import os

import click
import yaml

# WARNING: be careful with what is imported here.  Any decorators that read
# config cannot be imported here (even transitively) since they will attempt to
# read the config object before it exists.
from klio.transforms import core as klio_transforms_core
from klio_core import config
from klio_core import options as core_options
from klio_core import utils as core_utils

from klio_exec import options
from klio_exec.commands import audit
from klio_exec.commands import stop


# TODO remove 'INFO:root:' bit from cli status logs.
# TODO make 'ERROR' logs red colored?
logging.getLogger().setLevel(logging.INFO)


RuntimeConfig = collections.namedtuple(
    "RuntimeConfig", ["image_tag", "direct_runner", "update", "blocking"]
)


@click.group()
def main():
    pass  # pragma: no cover


def _get_config(config_path):
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except IOError as e:
        logging.error(e)
        raise SystemExit(1)


# TODO: remove when after internal calls are removed
def _compare_runtime_to_buildtime_config(klio_config):
    pass


@main.command("run")
@core_options.image_tag
@core_options.direct_runner
@options.blocking
@core_options.update
@core_utils.with_klio_config
def run_pipeline(
    image_tag, direct_runner, update, klio_config, config_meta, blocking
):

    # RunConfig ensures config is pickled and sent to worker.  Note this
    # depends on save_main_session being True
    # Notice, this is currently unused due to dataflow pickling issues, leaving
    # in for compatibility until a fix/alternative solution is in place
    klio_transforms_core.RunConfig.set(klio_config)

    # This can only be imported after RunConfig is set since it will end up
    # importing classes that may (or do) attempt to read it
    from klio_exec.commands import run

    if update is None:  # if it's not explicitly set in CLI, look at config
        update = klio_config.pipeline_options.update
    if blocking is None:  # if it's not explicitly set in CLI, look at config
        blocking = klio_config.job_config.blocking

    runtime_conf = RuntimeConfig(image_tag, direct_runner, update, blocking)

    klio_pipeline = run.KlioPipeline(
        klio_config.job_name, klio_config, runtime_conf
    )
    klio_pipeline.run()


@main.command("stop")
@options.config_file
def stop_job(config_file):
    job_dir = os.path.abspath(os.getcwd())
    config_file = config_file or "klio-job.yaml"
    config_path = os.path.join(job_dir, config_file)
    config_data = _get_config(config_path)
    conf_obj = config.KlioConfig(config_data)
    # TODO: make this a click option once draining is supported @lynn
    strategy = "cancel"
    stop.stop(conf_obj, strategy)


@main.command("test", context_settings=dict(ignore_unknown_options=True))
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
def test_job(pytest_args):
    """Thin wrapper around pytest. Any arguments after -- are passed through.
    """
    import os
    import pytest

    config_path = "klio-job.yaml"
    config_data = _get_config(config_path)
    conf_obj = config.KlioConfig(config_data)

    # RunConfig ensures config is pickled and sent to worker.  Note this
    # depends on save_main_session being True
    klio_transforms_core.RunConfig.set(conf_obj)

    # NOTE: we assume that test_job is the only method called in this
    # subprocess, so setting KLIO_TEST_MODE will not impact subsequent
    # method calls
    os.environ["KLIO_TEST_MODE"] = "true"
    exit_code = pytest.main(list(pytest_args))

    if exit_code != 0:
        raise SystemExit("Tests failed with exit code %s" % exit_code)


def _require_profile_input_data(input_file, entity_ids):
    # Note: can't use something like MutuallyExclusiveOption since entity IDs
    #       are click arguments, not options
    if not any([input_file, entity_ids]):
        msg = "Must provide `--input-file` or a list of entity IDs."
        raise click.UsageError(msg)

    if all([input_file, entity_ids]):
        msg = (
            "Illegal usage: `--input-file` is mutually exclusive with "
            "entity ID arguments."
        )
        raise click.UsageError(msg)


@main.group(
    "profile",
    help=(
        "Profile a job. NOTE: Requires klio-exec[debug] installed in the "
        "job's Docker image."
    ),
)
def profile_job():
    pass


# hidden command to only be invoked via a subprocess from
# `klioexec profile memory` or `klioexec profile cpu`
@profile_job.command("run-pipeline", hidden=True)
@options.input_file
@core_options.show_logs
@options.config_file
@click.argument("entity_ids", nargs=-1, required=False)
def _run_pipeline(input_file, show_logs, entity_ids, config_file):

    from klio_exec.commands import profile

    config_path = config_file or "klio-job.yaml"
    config_data = _get_config(config_path)
    klio_config = config.KlioConfig(config_data)

    # safety check, even though it should be invoked by another klioexec
    # command
    _require_profile_input_data(input_file, entity_ids)

    if not show_logs:
        logging.disable(logging.CRITICAL)

    klio_pipeline = profile.KlioPipeline(
        klio_config=klio_config, input_file=input_file, entity_ids=entity_ids
    )
    klio_pipeline.profile(what="run")


@profile_job.command(
    "memory",
    short_help="Profile overall memory usage.",
    help=(
        "Profile overall memory usage on an interval while running all "
        "Klio-based transforms."
    ),
)
@core_options.interval
@core_options.include_children
@core_options.multiprocess
@core_options.plot_graph
@options.input_file
@options.output_file
@core_options.show_logs
@options.config_file
@click.argument("entity_ids", nargs=-1, required=False)
def profile_memory(
    interval,
    include_children,
    multiprocess,
    input_file,
    output_file,
    plot_graph,
    show_logs,
    entity_ids,
    config_file,
):

    from klio_exec.commands import profile

    config_path = config_file or "klio-job.yaml"
    config_data = _get_config(config_path)
    klio_config = config.KlioConfig(config_data)

    _require_profile_input_data(input_file, entity_ids)

    klio_pipeline = profile.KlioPipeline(
        klio_config=klio_config,
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids,
    )
    kwargs = {
        "include_children": include_children,
        "multiprocess": multiprocess,
        "interval": interval,
        "show_logs": show_logs,
        "plot_graph": plot_graph,
    }
    output_png = klio_pipeline.profile(what="memory", **kwargs)
    if output_png:
        click.echo("Memory plot graph generated at: {}".format(output_png))


@profile_job.command(
    "memory-per-line",
    short_help="Profile memory usage per line.",
    help=(
        "Profile memory per line for every Klio-based transforms' process "
        "method."
    ),
)
@core_options.maximum
@core_options.per_element(show_default="True")
@options.input_file
@options.output_file
@core_options.show_logs
@options.config_file
@click.argument("entity_ids", nargs=-1, required=False)
def profile_memory_per_line(
    get_maximum,
    per_element,
    input_file,
    output_file,
    show_logs,
    entity_ids,
    config_file,
):
    from klio_exec.commands import profile

    config_path = config_file or "klio-job.yaml"
    config_data = _get_config(config_path)
    klio_config = config.KlioConfig(config_data)

    _require_profile_input_data(input_file, entity_ids)

    if not show_logs:
        logging.disable(logging.CRITICAL)

    klio_pipeline = profile.KlioPipeline(
        klio_config=klio_config,
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids,
    )
    klio_pipeline.profile(what="memory_per_line", get_maximum=get_maximum)


@profile_job.command(
    "cpu",
    short_help="Profile overall CPU usage.",
    help=(
        "Profile overall CPU usage on an interval while running all "
        "Klio-based transforms."
    ),
)
@core_options.interval
@options.input_file
@options.output_file
@core_options.plot_graph
@core_options.show_logs
@options.config_file
@click.argument("entity_ids", nargs=-1, required=False)
def profile_cpu(
    interval,
    input_file,
    output_file,
    plot_graph,
    show_logs,
    entity_ids,
    config_file,
):
    from klio_exec.commands import profile

    config_path = config_file or "klio-job.yaml"
    config_data = _get_config(config_path)
    klio_config = config.KlioConfig(config_data)

    _require_profile_input_data(input_file, entity_ids)

    klio_pipeline = profile.KlioPipeline(
        klio_config=klio_config,
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids,
    )
    kwargs = {
        "interval": interval,
        "show_logs": show_logs,
        "plot_graph": plot_graph,
    }
    output_png = klio_pipeline.profile(what="cpu", **kwargs)
    if output_png:
        click.echo("CPU plot graph generated at: {}".format(output_png))


@profile_job.command(
    "timeit",
    short_help="Profile wall time per line.",
    help=(
        "Profile wall time by every line for every Klio-based transforms' "
        "process method. NOTE: this uses the `line_profiler` package, not "
        "Python's `timeit` module."
    ),
)
@options.input_file
@options.output_file
@core_options.iterations
@core_options.show_logs
@options.config_file
@click.argument("entity_ids", nargs=-1, required=False)
def profile_wall_time(
    input_file, output_file, iterations, show_logs, entity_ids, config_file
):
    from klio_exec.commands import profile

    config_path = config_file or "klio-job.yaml"
    config_data = _get_config(config_path)
    klio_config = config.KlioConfig(config_data)

    _require_profile_input_data(input_file, entity_ids)

    if not show_logs:
        logging.disable(logging.CRITICAL)

    klio_pipeline = profile.KlioPipeline(
        klio_config=klio_config,
        input_file=input_file,
        output_file=output_file,
        entity_ids=entity_ids,
    )
    klio_pipeline.profile(what="timeit", iterations=iterations)


@main.command("audit", context_settings=dict(ignore_unknown_options=True))
@options.config_file
@click.option(
    "--list",
    is_flag=True,
    is_eager=True,
    expose_value=False,  # don't need to pass in parameter to audit_job func
    callback=audit.list_audit_steps,
    help="List available audit steps (does not run any audits).",
)
def audit_job(config_file):
    # NOTE: we assume that audit_job is the only method called in this
    # subprocess, so setting KLIO_TEST_MODE will not impact subsequent
    # method calls
    os.environ["KLIO_TEST_MODE"] = "true"

    job_dir = os.path.abspath(os.getcwd())
    config_data = _get_config(config_file or "klio-job.yaml")
    conf_obj = config.KlioConfig(config_data)
    audit.audit(job_dir, conf_obj)


if __name__ == "__main__":
    main()

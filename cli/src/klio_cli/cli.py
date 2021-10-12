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
"""Main entrypoint for creating, running, & maintaining a Klio job."""

import collections
import logging
import os
import sys
import warnings

import click

from klio_core import options as core_options
from klio_core import utils as core_utils
from klio_core import variables as var

from klio_cli import __version__ as version
from klio_cli import options
from klio_cli.commands import image as image_commands
from klio_cli.commands import job as job_commands
from klio_cli.commands import message as message_commands
from klio_cli.utils import cli_utils
from klio_cli.utils import config_utils


# Google assumes this is a service, and not a CLI tool. It warns multiple
# times, polluting our logging, and making it overall confusing to read.
if not sys.warnoptions:
    warnings.filterwarnings(
        "ignore",
        category=UserWarning,
        message="Your application has authenticated*",
    )


# TODO: configure a CLI logger so that it's more pleasing to the user,
#       including removing `INFO:root:` prefix, add color-coded log
#       lines, and make it obvious where logs from 3rd-party libraries
#       (i.e. `docker` and `beam`) (@jpvelez)
logging.getLogger().setLevel(logging.INFO)


DockerRuntimeConfig = collections.namedtuple(
    "DockerRuntimeConfig", ["image_tag", "force_build", "config_file_override"]
)
# TODO: We can get rid of direct_runner since we have runner
RunJobConfig = collections.namedtuple(
    "RunJobConfig", ["direct_runner", "update", "git_sha"]
)
ProfileConfig = collections.namedtuple(
    "ProfileConfig", ["input_file", "output_file", "show_logs", "entity_ids"],
)


#####
# CLI groupings/top-level commands (`klio`, `klio job`, `klio image` etc)
#####
@click.group()
@click.version_option(version, prog_name="klio-cli")  # instead of "klio"
def main():
    pass


@main.group("job")
def job():
    """Create and manage Klio jobs."""
    pass


@main.group("image")
def image():
    """Manage a job's Docker image."""
    pass


@main.group("message")
def message():
    """Manage a job's message queue via Pub/Sub."""
    pass


# CLI subcommands
@job.group(
    "profile",
    help=(
        "Profile a job.\n\n**NOTE:** Requires ``klio-exec[debug]`` installed "
        "in the job's Docker image."
    ),
)
def profile():
    pass


@job.group(
    "config", help=("View and edit a Klio job's configuration."),
)
def configuration():
    pass


#####
# `klio image` commands
#####
@image.command("build", help="Build the Docker worker image.")
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@core_utils.with_klio_config
def build_image(klio_config, config_meta, **kwargs):
    if not kwargs.get("image_tag"):
        kwargs["image_tag"] = cli_utils.get_git_sha(config_meta.job_dir)

    image_commands.build.build(
        config_meta.job_dir, klio_config, config_meta.config_file, **kwargs
    )


#####
# `klio job` commands
#####
@job.command("run", help="Run a klio job.")
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.runtime
@core_utils.with_klio_config
def run_job(klio_config, config_meta, **kwargs):
    direct_runner = cli_utils.is_direct_runner(
        klio_config, kwargs.pop("direct_runner")
    )
    cli_utils.error_stackdriver_logger_metrics(klio_config, direct_runner)

    git_sha = cli_utils.get_git_sha(
        config_meta.job_dir, kwargs.get("image_tag")
    )
    image_tag = kwargs.get("image_tag") or git_sha

    runtime_config = DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=kwargs.get("force_build"),
        config_file_override=config_meta.config_file,
    )

    run_job_config = RunJobConfig(
        direct_runner=direct_runner,
        update=kwargs.pop("update"),
        git_sha=git_sha,
    )

    if (
        not direct_runner
        and klio_config.pipeline_options.runner
        == var.KlioRunner.DIRECT_GKE_RUNNER
    ):
        gke_commands = cli_utils.import_gke_commands()

        klio_pipeline = gke_commands.RunPipelineGKE(
            config_meta.job_dir, klio_config, runtime_config, run_job_config
        )
    else:
        klio_pipeline = job_commands.run.RunPipeline(
            config_meta.job_dir, klio_config, runtime_config, run_job_config
        )
    rc = klio_pipeline.run()
    sys.exit(rc)


@job.command(
    "stop",
    help=(
        "Cancel a currently running job.\n\n**NOTE:** Draining is not supported"
    ),
)
@core_options.job_dir
@core_options.config_file
@options.job_name(
    help=(
        "Name of job, if neither ``--job-dir`` nor ``--config-file`` is not "
        "provided."
    )
)
@options.region
@options.gcp_project(
    help=(
        "Project of job, if neither ``--job-dir`` nor ``--config-file`` is not "
        "provided."
    )
)
@core_utils.with_klio_config
def stop_job(klio_config, config_meta, job_name, region, gcp_project):
    if job_name and any([config_meta.job_dir, config_meta.config_file]):
        logging.error(
            "'--job-name' can not be used with '--config-file' and/or "
            "'--job-dir'."
        )
        raise SystemExit(1)

    if job_name and not all([gcp_project, region]):
        logging.error(
            "Both '--region' and '--gcp-project' are required when using the "
            "'--job-name' option."
        )
        raise SystemExit(1)

    if not job_name:
        job_name = klio_config.job_name
        gcp_project = klio_config.pipeline_options.project
        region = klio_config.pipeline_options.region

    # TODO: make this a click option once draining is supported @lynn
    strategy = "cancel"
    if klio_config.pipeline_options.runner == var.KlioRunner.DIRECT_GKE_RUNNER:
        gke_commands = cli_utils.import_gke_commands()

        gke_commands.StopPipelineGKE(config_meta.job_dir).stop()
    else:
        job_commands.stop.StopJob().stop(
            job_name, gcp_project, region, strategy
        )


@job.command(
    "deploy",
    help=(
        "Deploy a job. This will first cancel any currently running job of "
        "the same name & region.\n\n**NOTE:** Draining is not supported."
    ),
)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.runtime
@core_utils.with_klio_config
def deploy_job(klio_config, config_meta, **kwargs):
    direct_runner = cli_utils.is_direct_runner(
        klio_config, kwargs.pop("direct_runner")
    )
    cli_utils.error_stackdriver_logger_metrics(klio_config, direct_runner)

    git_sha = cli_utils.get_git_sha(
        config_meta.job_dir, kwargs.get("image_tag")
    )
    image_tag = kwargs.get("image_tag") or git_sha
    if config_meta.config_file:
        basename = os.path.basename(config_meta.config_file)
        image_tag = "{}-{}".format(image_tag, basename)

    runtime_config = DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=kwargs.get("force_build"),
        config_file_override=config_meta.config_file,
    )

    run_job_config = RunJobConfig(
        direct_runner=direct_runner,
        update=kwargs.pop("update"),
        git_sha=git_sha,
    )
    # TODO: make this a click option once draining is supported @lynn
    if not run_job_config.update:
        job_name = klio_config.job_name
        gcp_project = klio_config.pipeline_options.project
        region = klio_config.pipeline_options.region
        strategy = "cancel"
        job_commands.stop.StopJob().stop(
            job_name, gcp_project, region, strategy
        )

    if (
        not direct_runner
        and klio_config.pipeline_options.runner
        == var.KlioRunner.DIRECT_GKE_RUNNER
    ):
        gke_commands = cli_utils.import_gke_commands()

        run_command = gke_commands.RunPipelineGKE(
            config_meta.job_dir, klio_config, runtime_config, run_job_config
        )
    else:
        run_command = job_commands.run.RunPipeline(
            config_meta.job_dir, klio_config, runtime_config, run_job_config
        )
    rc = run_command.run()
    sys.exit(rc)


def _validate_job_name(ctx, param, value):
    # TODO validate name for apache beam/dataflow & python imports/packaging
    #      (unknown what valid is)  (@lynn)
    return value


@job.command(
    "create",
    help=("Create the necessary files for a new Klio job."),
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
        allow_interspersed_args=True,
    ),
)
@options.job_name(
    prompt="Name of your new job",
    help="Name of your new job",
    callback=_validate_job_name,
)
# TODO: grab default project from gcloud config or env var, and only
#       prompt if not available (but allow overriding) (@lynn)
@options.gcp_project(
    prompt="Name of the GCP project the job should be created in",
    help="Name of the GCP project the job should be created in",
)
@options.output
@options.use_defaults
@click.argument("addl_job_opts", nargs=-1, type=click.UNPROCESSED)
def create_job(addl_job_opts, output, **known_kwargs):
    if not output:
        output = os.getcwd()
    output = os.path.abspath(output)
    job = job_commands.create.CreateJob()
    job.create(addl_job_opts, known_kwargs, output)


@job.command(
    "delete", help=("Delete GCP-related resources created by a Klio job")
)
@core_utils.with_klio_config
def delete_job(klio_config, config_meta):
    if klio_config.pipeline_options.runner == var.KlioRunner.DIRECT_GKE_RUNNER:
        gke_commands = cli_utils.import_gke_commands()

        gke_commands.DeletePipelineGKE(config_meta.job_dir).delete()
    else:
        job_commands.delete.DeleteJob(klio_config).delete()


@job.command(
    "test",
    help="Run unit tests for job.",
    # set this so that pytest options can be pass-thru
    context_settings=dict(ignore_unknown_options=True),
)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.force_build
@click.argument("pytest_args", nargs=-1, type=click.UNPROCESSED)
@core_utils.with_klio_config
def test_job(
    klio_config, config_meta, force_build, image_tag, pytest_args, **kwargs
):
    """Thin wrapper around pytest. Any arguments after -- are passed through."""
    pytest_args = list(pytest_args)

    if pytest_args:
        # test calls klioexec, which also uses click.
        # click relies on the presence of -- to process option-like arguments
        # such as -s
        pytest_args.insert(0, "--")

    if not image_tag:
        image_tag = cli_utils.get_git_sha(config_meta.job_dir)

    if config_meta.config_file:
        image_tag = "{}-{}".format(
            image_tag, os.path.basename(config_meta.config_file)
        )

    runtime_config = DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=force_build,
        config_file_override=config_meta.config_file,
    )

    pipeline = job_commands.test.TestPipeline(
        config_meta.job_dir, klio_config, runtime_config
    )
    rc = pipeline.run(pytest_args=pytest_args)
    sys.exit(rc)


@job.command(
    "verify",
    short_help="Verify a job's required GCP resources exist.",
    help=(
        "Verifies all GCP resources and dependencies used in the job "
        "so that the Klio Job as defined in the ``klio-info.yaml`` can run "
        "properly in production."
    ),
)
@options.create_resources
@core_utils.with_klio_config
def verify_job(klio_config, config_meta, create_resources):
    job = job_commands.verify.VerifyJob(klio_config, create_resources)
    job.verify_job()


@job.command(
    "audit",
    short_help="Audit a job for common issues.",
    help=(
        "Audit a job for detect common issues via running tests with "
        "additional mocking.\n\n**NOTE:** Additional arguments to pytest are "
        "not supported."
    ),
)
@options.force_build
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.list_steps
@core_utils.with_klio_config
def audit_job(klio_config, config_meta, force_build, image_tag, list_steps):

    git_sha = cli_utils.get_git_sha(config_meta.job_dir, image_tag)
    image_tag = image_tag or git_sha
    if config_meta.config_file:
        basename = os.path.basename(config_meta.config_file)
        image_tag = "{}-{}".format(image_tag, basename)

    runtime_config = DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=force_build,
        config_file_override=config_meta.config_file,
    )

    pipeline = job_commands.audit.AuditPipeline(
        job_dir=config_meta.job_dir,
        klio_config=klio_config,
        docker_runtime_config=runtime_config,
    )

    rc = pipeline.run(list_steps)
    sys.exit(rc)


#####
# `klio job config` subcommands
#####
def _job_config(job_dir, config_file, verb, *args, **kwargs):
    _, config_path = core_utils.get_config_job_dir(job_dir, config_file)

    effective_job_config = job_commands.configuration.EffectiveJobConfig(
        config_path
    )
    func = getattr(effective_job_config, verb)
    func(*args, **kwargs)


@configuration.command(
    "show", help="Show the complete effective configuration for a Klio job.",
)
@core_options.job_dir
@core_options.config_file
def show_job_config(job_dir, config_file):
    _job_config(job_dir, config_file, "show")


@configuration.command(
    "set",
    short_help="Set a configuration value for a Klio job.",
    help=(
        "Set a configuration value for a Klio job. Multiple "
        "pairs of ``SECTION.PROPERTY=VALUE`` are accepted."
    ),
)
@core_options.job_dir
@core_options.config_file
@click.argument(
    "target_to_value",
    required=True,
    metavar="SECTION.PROPERTY=VALUE...",
    nargs=-1,
)
def set_job_config(job_dir, config_file, target_to_value):
    _job_config(job_dir, config_file, "set", target_to_value)


@configuration.command(
    "unset", help="Unset a configuration value for a Klio job.",
)
@core_options.job_dir
@core_options.config_file
@click.argument("section_property", required=True, metavar="SECTION.PROPERTY")
def unset_job_config(job_dir, config_file, section_property):
    _job_config(job_dir, config_file, "unset", section_property)


@configuration.command(
    "get", help="Get the value for a configuration property of a Klio job.",
)
@core_options.job_dir
@core_options.config_file
@click.argument("section_property", required=True, metavar="SECTION.PROPERTY")
def get_job_config(job_dir, config_file, section_property):
    _job_config(job_dir, config_file, "get", section_property)


#####
# `klio job profile` subcommands
#####
@profile.command(
    "collect-profiling-data",
    short_help="Collect & view Dataflow profiling output from GCS.",
    help=(
        "Collect & view profiling output in GCS. Sorting and restrictions as "
        "supported by `the stats class <https://docs.python.org/3/library/"
        "profile.html#the-stats-class>`_. \n\n**NOTE:** This requires running "
        "the Klio job on Dataflow with ``pipeline_options.profile_location`` "
        "set to a GCS bucket, and either/both ``pipeline_options.profile_cpu``"
        " and/or ``pipeline_options.profile_memory`` set to ``True`` in "
        "``klio-job.yaml``."
    ),
)
@core_options.job_dir(mutex=["gcs_location", "input_file"])
@core_options.config_file(mutex=["gcs_location", "input_file"])
@options.gcs_location
@options.since
@options.until
@options.input_file(
    mutex=["output_file", "gcs_location", "job_dir", "config_file"],
    help="Print stats from a previously-saved output.",
)
@options.output_file(
    mutex=["input_file"],
    help="Dump collected cProfile data to a desired output file.",
)
@options.sort_stats
@click.argument(
    "restrictions", nargs=-1, required=False, type=click.UNPROCESSED
)
@core_utils.with_klio_config
def collect_profiling_data(
    klio_config,
    config_meta,
    gcs_location,
    since,
    until,
    sort_stats,
    input_file,
    output_file,
    restrictions,
):
    if input_file and any([since, until]):
        msg = (
            "Options `--since` and `--until`  will be ignored when using the "
            "option `--input-file`."
        )
        click.secho(msg, fg="yellow")
    if not since:
        since = "1 hour ago"
    if not until:
        until = "now"

    if not gcs_location:
        gcs_location = klio_config.pipeline_options.profile_location

        if not gcs_location:
            msg = (
                "Please provide a GCS location of the profiling data, either "
                "via the `--gcs-location` option flag or in `klio-job.yaml` "
                "under `pipeline_options.profile_location`."
            )
            raise click.UsageError(msg)

    gcs_profile_collector = job_commands.profile.DataflowProfileStatsCollector(
        gcs_location=gcs_location,
        input_file=input_file,
        output_file=output_file,
        since=since,
        until=until,
    )
    gcs_profile_collector.get(sort_stats, restrictions)


# TODO: same as in exec/src/klio_exec/cli.py; move to core @lynn
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


def _profile(subcommand, klio_config, config_meta, **kwargs):
    _require_profile_input_data(kwargs["input_file"], kwargs["entity_ids"])

    image_tag = kwargs.pop("image_tag", None)
    if not image_tag:
        image_tag = cli_utils.get_git_sha(config_meta.job_dir)

    if config_meta.config_file:
        image_tag = "{}-{}".format(
            image_tag, os.path.basename(config_meta.config_file)
        )

    runtime_config = DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=kwargs.pop("force_build", None),
        config_file_override=config_meta.config_file,
    )
    profile_config = ProfileConfig(
        input_file=kwargs.pop("input_file", None),
        output_file=kwargs.pop("output_file", None),
        show_logs=kwargs.pop("show_logs", None),
        entity_ids=kwargs.pop("entity_ids", None),
    )

    pipeline = job_commands.profile.ProfilePipeline(
        config_meta.job_dir, klio_config, runtime_config, profile_config
    )
    pipeline.run(what=subcommand, subcommand_flags=kwargs)


@profile.command(
    "memory",
    short_help="Profile overall memory usage.",
    help=(
        "Profile overall memory usage on an interval while running all "
        "Klio-based transforms."
    ),
)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.force_build
@core_options.interval
@core_options.include_children
@core_options.multiprocess
@core_options.plot_graph
@options.input_file(
    help=(
        "File of entity IDs (separated by a new line character) with "
        "which to profile a Klio job. If file path is not absolute, it will be "
        "treated relative to ``--job-dir``."
    ),
)
@options.output_file(help="Output file for results. [default: stdout]")
@core_options.show_logs
@click.argument("entity_ids", nargs=-1, required=False)
@core_utils.with_klio_config
def profile_memory(klio_config, config_meta, **kwargs):
    _profile("memory", klio_config, config_meta, **kwargs)


@profile.command(
    "memory-per-line",
    short_help="Profile memory usage per line.",
    help=(
        "Profile memory per line for every Klio-based transforms' process "
        "method."
    ),
)
@core_options.maximum
@core_options.per_element(show_default=True)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.force_build
@options.input_file(
    help=(
        "File of entity IDs (separated by a new line character) with "
        "which to profile a Klio job. If file path is not absolute, it will be "
        "treated relative to ``--job-dir``."
    ),
)
@options.output_file(help="Output file for results. [default: stdout]")
@core_options.show_logs
@click.argument("entity_ids", nargs=-1, required=False)
@core_utils.with_klio_config
def profile_memory_per_line(klio_config, config_meta, **kwargs):
    _profile("memory-per-line", klio_config, config_meta, **kwargs)


@profile.command(
    "cpu",
    short_help="Profile overall CPU usage.",
    help=(
        "Profile overall CPU usage on an interval while running all "
        "Klio-based transforms."
    ),
)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.force_build
@core_options.interval
@core_options.plot_graph
@options.input_file(
    help=(
        "File of entity IDs (separated by a new line character) with "
        "which to profile a Klio job. If file path is not absolute, it will be "
        "treated relative to ``--job-dir``."
    ),
)
@options.output_file(help="Output file for results. [default: stdout]")
@core_options.show_logs
@click.argument("entity_ids", nargs=-1, required=False)
@core_utils.with_klio_config
def profile_cpu(klio_config, config_meta, **kwargs):
    _profile("cpu", klio_config, config_meta, **kwargs)


@profile.command(
    "timeit",
    short_help="Profile wall time per line.",
    help=(
        "Profile wall time by every line for every Klio-based transforms' "
        "process method. \n\n**NOTE:** this uses the ``line_profiler`` "
        "package, not Python's ``timeit`` module."
    ),
)
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.force_build
@options.input_file(
    help=(
        "File of entity IDs (separated by a new line character) with "
        "which to profile a Klio job. If file path is not absolute, it will be "
        "treated relative to ``--job-dir``."
    ),
)
@options.output_file(help="Output file for results. [default: stdout]")
@core_options.iterations
@core_options.show_logs
@click.argument("entity_ids", nargs=-1, required=False)
@core_utils.with_klio_config
def profile_timeit(klio_config, config_meta, **kwargs):
    _profile("timeit", klio_config, config_meta, **kwargs)


#####
# `klio message` commands
#####
@message.command("publish", help="Publish a message(s) to a running job.")
@options.force
@options.ping
@options.top_down
@options.bottom_up
@options.non_klio
@click.argument("entity_ids", nargs=-1, required=True)
@core_utils.with_klio_config
def publish(
    entity_ids,
    force,
    ping,
    top_down,
    bottom_up,
    non_klio,
    klio_config,
    config_meta,
):
    if top_down and bottom_up:
        msg = "Must either use `--top-down` or `--bottom-up`, not both."
        raise click.UsageError(msg)

    # TODO: make sure this is moved into KlioConfig
    config_utils.set_config_version(klio_config)

    if non_klio:
        if any([force, ping, bottom_up]):
            msg = (
                "Can not publish a non-Klio message using --force, --ping, "
                "or --bottom-up flags."
            )
            raise click.UsageError(msg)

        allow_non_klio = klio_config.job_config.allow_non_klio_messages
        # The job itself will also error and drop the message in case messages
        # aren't published through the CLI; this is just a safety check.
        if not allow_non_klio:
            logging.error(
                "You're trying to publish a non-Klio message to a job that "
                "does not support free-form messages. To support non-Klio "
                "messages, set `job_config.allow_non_klio_messages = true` "
                "in the job's `klio-job.yaml` file, and re-deploy."
            )
            raise SystemExit(1)
    message_commands.publish.publish_messages(
        klio_config, entity_ids, force, ping, top_down, non_klio
    )

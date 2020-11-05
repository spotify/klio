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

import os

import click
import klio_core.options as core_options

from klio_cli import cli as main_cli
from klio_cli import options
from klio_cli.cli import main
from klio_cli.utils import cli_utils
from klio_cli.utils import config_utils
from klio_core import config

from klio_devtools.commands import develop


@main.command(
    "develop",
    short_help="Develop on the klio ecosystem in a job's container.",
    help=(
        "Builds & runs a job's container, mounts the job's code in "
        "`/usr/src/app`, installs klio packages as 'editable' packages "
        "that will automatically pick up local changes, and attaches to "
        "the container with an interactive terminal to enable manual "
        "runs of `klioexec`.\n\nNOTE: It's probably a good idea to locally "
        "bump the versions of the libraries to ensure proper installation."
    ),
)
@options.job_dir
@options.config_file
@core_options.image_tag(default=None, show_default="``git-sha[dirty?]``")
@options.runtime
@click.option(
    "--klio-path",
    type=click.Path(
        exists=True,
        dir_okay=True,
        file_okay=False,
        readable=True,
        writable=True,
        resolve_path=True,
    ),
    help="Path to klio repo",
    required=True,
)
@click.option(
    "--exclude", help="exclude installing a particular package", multiple=True,
)
def develop_job(job_dir, config_file, **kwargs):
    job_dir, config_path = cli_utils.get_config_job_dir(job_dir, config_file)
    config_data = config_utils.get_config_by_path(config_path)
    conf = config.KlioConfig(config_data)

    git_sha = cli_utils.get_git_sha(job_dir, kwargs.get("image_tag"))
    image_tag = kwargs.get("image_tag") or git_sha
    if config_file:
        basename = os.path.basename(config_file)
        image_tag = "{}-{}".format(image_tag, basename)

    runtime_config = main_cli.DockerRuntimeConfig(
        image_tag=image_tag,
        force_build=kwargs.get("force_build"),
        config_file_override=config_file,
    )

    klio_pipeline = develop.DevelopKlioContainer(
        job_dir, conf, runtime_config, kwargs["klio_path"], kwargs["exclude"]
    )
    klio_pipeline.run()

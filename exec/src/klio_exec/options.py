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
# options for `klioexec run`
#####


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

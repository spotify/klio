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

import functools
import logging
import os
import subprocess
import warnings

import attr

from klio_core import config

from klio_cli import options
from klio_cli.utils import config_utils


def get_git_sha(cwd=None, image_tag=None):

    cmd = "git describe --match=NeVeRmAtCh --always --abbrev=8 --dirty"
    try:
        return (
            subprocess.check_output(
                # pipe to devnull to suppress the error msgs from git itself
                cmd.split(),
                cwd=cwd,
                stderr=subprocess.DEVNULL,
            )
            .decode()
            .strip()
        )
    except subprocess.CalledProcessError:
        if not image_tag:
            logging.error(
                "The directory from which you are running this is not a git "
                "directory, or has no commits yet. The latest commit is used "
                "to tag the Docker image that is built by this command. "
                "Consider overriding this value using the --image-tag flag "
                "until such a time as commits are available."
            )
            raise SystemExit(1)


# TODO: Move this to KlioConfig validation
#  once overriding & templates are done
def validate_dataflow_runner_config(klio_config):
    pipeline_opts = klio_config.pipeline_options.as_dict()
    mandatory_gcp_keys = [
        "project",
        "staging_location",
        "temp_location",
        "region",
    ]
    is_gcp = all(
        pipeline_opts.get(key) is not None for key in mandatory_gcp_keys
    )

    if not is_gcp:
        logging.error(
            "Unable to verify the mandatory configuration fields for"
            " DataflowRunner. Please fix job configuration or run via direct"
            "runner."
        )
        raise SystemExit(1)


def is_direct_runner(klio_config, direct_runner):
    if not direct_runner:
        validate_dataflow_runner_config(klio_config)

    return direct_runner


def warn_if_py2_job(job_dir):
    dockerfile_path = os.path.join(job_dir, "Dockerfile")
    from_line = None
    with open(dockerfile_path, "r") as f:
        for line in f.readlines():
            if line.startswith("FROM"):
                from_line = line
                break
    if not from_line:
        # not having a FROM line will break elsewhere, so no need to take
        # care of it here
        return

    py2_dataflow_images = [
        "dataflow.gcr.io/v1beta3/python",
        "dataflow.gcr.io/v1beta3/python-base",
        "dataflow.gcr.io/v1beta3/python-fnapi",
    ]
    from_image = from_line.lstrip("FROM ").split(":")[0]
    if from_image in py2_dataflow_images:
        msg = (
            "Python 2 support in Klio is deprecated. Please upgrade "
            "to Python 3.5+."
        )
        warnings.warn(msg, category=UserWarning)


def get_config_job_dir(job_dir, config_file):
    if job_dir and config_file:
        config_file = os.path.join(job_dir, config_file)

    elif not job_dir:
        job_dir = os.getcwd()

    job_dir = os.path.abspath(job_dir)

    if not config_file:
        config_file = os.path.join(job_dir, "klio-job.yaml")

    return job_dir, config_file


@attr.attrs
class KlioConfigMeta(object):

    # resolved directory of job
    job_dir = attr.attrib()

    # resolved path to config file
    config_path = attr.attrib()

    # user-override of config file (may be None)
    config_file = attr.attrib()


def with_klio_config(func):
    """Decorator for commands to automatically handle a number of options
    regarding config, and provide a properly constructed KlioConfig as an
    argument named `klio_config`.

    Be aware this is a function wrapper and must come after any other
    decorators for click options, arguments, etc.
    """

    @options.override
    @options.template
    @options.job_dir
    @options.config_file
    def wrapper(*args, **kwargs):
        raw_overrides = kwargs.pop("override")
        raw_templates = kwargs.pop("template")
        job_dir = kwargs.pop("job_dir")
        config_file = kwargs.pop("config_file")
        job_dir, config_path = get_config_job_dir(job_dir, config_file)

        warn_if_py2_job(job_dir)

        raw_config_data = config_utils.get_config_by_path(config_path)

        processed_config_data = config.KlioConfigPreprocessor.process(
            raw_config_data=raw_config_data,
            raw_template_list=raw_templates,
            raw_override_list=raw_overrides,
        )

        meta = KlioConfigMeta(
            job_dir=job_dir, config_file=config_file, config_path=config_path,
        )

        conf = config.KlioConfig(processed_config_data)

        kwargs["klio_config"] = conf
        kwargs["config_meta"] = meta

        func(*args, **kwargs)

    return functools.update_wrapper(wrapper, func)

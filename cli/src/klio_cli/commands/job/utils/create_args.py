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

import re

import attr
import click

VALID_BEAM_PY_VERSIONS = ["3.5", "3.6", "3.7", "3.8"]
VALID_BEAM_PY_VERSIONS_SHORT = [
    "".join(p.split(".")) for p in VALID_BEAM_PY_VERSIONS
]
GCP_REGIONS = [
    "us-west1",
    "us-central1",
    "us-east1",
    "us-east4",
    "northamerica-northeast1",
    "europe-west2",
    "europe-west1",
    "europe-west4",
    "europe-west3",
    "asia-southeast1",
    "asia-east1",
    "asia-northeast1",
    "australia-southeast1",
]


BASE_TOPIC_TPL = "projects/{gcp_project}/topics/{job_name}"
WORKER_IMAGE_TPL = "gcr.io/{gcp_project}/{job_name}-worker"

# Valid project IDs: https://cloud.google.com/resource-manager/reference
#                    /rest/v1/projects#Project
# Valid topic names: https://cloud.google.com/pubsub/docs/admin#resource_names
# Will save matches into symbolic groups named "project" and "topic"
TOPIC_REGEX = re.compile(
    r"^projects/(?P<project>[a-z0-9-]{6,30})/"
    r"topics/(?P<topic>[a-zA-Z0-9-_.~+%]{3,255})"
)


def convert_to_bool(booleanish):
    """Convert boolean-ish string to actual booleans.

    Args:
        booleanish(str, bool): String with a value
            that might be boolean, e.g. "y", "N"
    Returns:
          True if booleanish matches (case-insensitive)
               any of "y", "true", or "yes"
          True if booleanish is the boolean value True
          False otherwise.
    """
    if isinstance(booleanish, bool):
        return booleanish
    return booleanish.lower() in ["y", "true", "yes"]


def python_version_converter(python_version):
    """Return version that can be appended to name of worker image

    Args:
        python_version(str): Python version as input by user
                            or on the command line
    Returns:
       "3" if major.minor  is 3.5
       First 2 values w/o the separating "." otherwise (e.g. 36, 37)
    """
    # 3.x -> 3x; 3.x.y -> 3x
    if "." in python_version:
        python_version = "".join(python_version.split(".")[:2])

    # Dataflow's 3.5 image is just "python3" while 3.6 and 3.7 are
    # "python36" and "python37"
    if python_version == "35":
        python_version = "3"
    return python_version


def experiments_converter(value):
    """Convert comma separated string to list if needed"""
    if isinstance(value, list):
        return value
    return value.split(",")


class CreateJobArgsValidationError(Exception):
    """Argument validation failed"""


@attr.s(on_setattr=[attr.setters.convert, attr.setters.validate])
class CreateJobArgs(object):
    """Container object for `klio job create` arguments used during

    Provides logic for argument normalization, validation and
    default-setting.

    Args:
        job_name(str): Name of the job. Required.
        gcp_project(str): Name of GCP project this job runs in. Required.
    """

    job_name = attr.ib()
    gcp_project = attr.ib()

    worker_image = attr.ib(converter=attr.converters.optional(str))
    create_resources = attr.ib(
        converter=attr.converters.optional(convert_to_bool), default=False
    )
    python_version = attr.ib(
        converter=attr.converters.optional(python_version_converter),
        default="36",
    )

    staging_location = attr.ib(converter=attr.converters.optional(str))
    temp_location = attr.ib(converter=attr.converters.optional(str))
    output_topic = attr.ib(converter=attr.converters.optional(str))
    output_data_location = attr.ib(converter=attr.converters.optional(str))
    input_topic = attr.ib(converter=attr.converters.optional(str))
    input_data_location = attr.ib(converter=attr.converters.optional(str))
    subscription = attr.ib(converter=attr.converters.optional(str))

    use_fnapi = attr.ib(converter=convert_to_bool, default=True)
    # if not use_fnapi, experiments should not include "beam_fn_api"
    # remember to change both if changing default for use_fnapi
    experiments = attr.ib(converter=experiments_converter)
    region = attr.ib(
        converter=attr.converters.optional(str), default="europe-west1"
    )
    num_workers = attr.ib(converter=attr.converters.optional(int), default=2)
    max_num_workers = attr.ib(
        converter=attr.converters.optional(int), default=2
    )
    autoscaling_algorithm = attr.ib(
        converter=attr.converters.optional(str), default="NONE"
    )
    disk_size_gb = attr.ib(converter=attr.converters.optional(int), default=32)
    worker_machine_type = attr.ib(
        converter=attr.converters.optional(str), default="n1-standard-2"
    )

    @classmethod
    def valid_fields_dict(cls):
        return attr.fields_dict(cls).keys()

    @classmethod
    def from_dict(cls, fields_dict):
        """Build CreateJobArgs instance from fields_dict

        Must include required fields (gcp_project, job_name)
        Ignores any unknown fields in fields_dict, and uses defaults
        wherever an optional field is not provided in fields_dict.

        Args:
              fields_dict(dict): Dictionary of CreateJobArgs fields values.
                Must include required fields (gcp_project, job_name)
        Returns:
            CreateJobArgs instance built using field_dict, leaving
            default values where field_dict does not provide them
        """

        create_job_args = cls(
            gcp_project=fields_dict.get("gcp_project"),
            job_name=fields_dict.get("job_name"),
        )

        create_job_args_fields = attr.fields_dict(cls).keys()
        for key in create_job_args_fields:
            value = fields_dict.get(key)
            if value:
                setattr(create_job_args, key, value)

        if "input_topic" in fields_dict and "subscription" not in fields_dict:
            # Default subscription depends on input topic,
            # so if a subscription isn't specified when the input topic is,
            # recalculate the default subscription
            create_job_args.subscription = create_job_args.get_default(
                "subscription"
            )

        return create_job_args

    @python_version.validator
    def _validate_python_version(self, attribute, python_version):
        """Python version validator

        Args:
             python_version(str): Shortened python version e.g. 36, 37
        Raises:
            CreateJobArgsValidation error if python_version is not
            in VALID_BEAM_PY_VERSIONS
        """
        if python_version.startswith("2"):
            msg = (
                "Klio no longer supports Python 2.7. "
                "Supported versions: {}".format(
                    ", ".join(VALID_BEAM_PY_VERSIONS)
                )
            )
            raise CreateJobArgsValidationError(msg)

        # valid examples: 35, 3.5, 3.5.6
        if python_version not in VALID_BEAM_PY_VERSIONS_SHORT:
            invalid_err_msg = (
                "Invalid Python version given: '{}'. Valid Python versions: "
                "{}".format(python_version, ", ".join(VALID_BEAM_PY_VERSIONS))
            )
            raise CreateJobArgsValidationError(invalid_err_msg)

    @region.validator
    def _validate_region(self, attribute, region):
        if region and region not in GCP_REGIONS:
            regions = ", ".join(GCP_REGIONS)
            msg = '"{0}" is not a valid region. Available: {1}'.format(
                region, regions
            )
            raise CreateJobArgsValidationError(msg)

    @experiments.default
    def _default_experiments(self):
        return ["beam_fn_api"]

    @worker_image.default
    def _default_worker_image(self):
        return WORKER_IMAGE_TPL.format(
            gcp_project=self.gcp_project, job_name=self.job_name
        )

    @property
    def _default_bucket(self):
        return "gs://{gcp_project}-dataflow-tmp/{job_name}".format(
            gcp_project=self.gcp_project, job_name=self.job_name
        )

    @temp_location.default
    def _default_temp_location(self):
        return "{bucket}/temp".format(bucket=self._default_bucket)

    @staging_location.default
    def _default_staging_location(self):
        return "{bucket}/staging".format(bucket=self._default_bucket)

    @property
    def _default_base_topic(self):
        return BASE_TOPIC_TPL.format(
            gcp_project=self.gcp_project, job_name=self.job_name
        )

    @output_topic.default
    def _default_output_topic(self):
        return "{base_topic}-output".format(
            base_topic=self._default_base_topic
        )

    @input_topic.default
    def _default_input_topic(self):
        return "{base_topic}-input".format(base_topic=self._default_base_topic)

    @subscription.default
    def _default_input_subscription(self):
        match = TOPIC_REGEX.match(self.input_topic)
        topic_name = match.group("topic")
        SUB_TPL = (
            "projects/{gcp_project}/subscriptions/{topic_name}-{job_name}"
        )
        return SUB_TPL.format(
            gcp_project=self.gcp_project,
            topic_name=topic_name,
            job_name=self.job_name,
        )

    @output_data_location.default
    def _default_output_data_location(self):
        return "gs://{gcp_project}-output/{job_name}".format(
            gcp_project=self.gcp_project, job_name=self.job_name
        )

    @input_data_location.default
    def _default_input_data_location(self):
        return "gs://{gcp_project}-input/{job_name}".format(
            gcp_project=self.gcp_project, job_name=self.job_name
        )

    def get_default(self, field):
        f = attr.fields_dict(self.__class__)[field]
        default = f.default
        if isinstance(default, attr._make.Factory):
            return default.factory(self)
        return default


class CreateJobPromptInput(object):
    def __init__(self, cmd_line_args):
        self.cmd_line_args = cmd_line_args
        # set it to None so that when we call `parse`,
        # it gives a hint that it's going
        # to be mutated
        self.create_job_args = None

    def set_region(self):
        region = self.cmd_line_args.get("region")
        if not region:
            region = click.prompt(
                "Desired GCP Region",
                default=self.create_job_args.get_default("region"),
            )
        self.create_job_args.region = region

    def set_use_fnapi(self):
        use_fnapi = self.cmd_line_args.get("use_fnapi")
        if not use_fnapi:
            use_fnapi = click.prompt(
                "Use Apache Beam's FnAPI (experimental) [Y/n]",
                type=click.Choice(["y", "Y", "n", "N"]),
                default="y"
                if self.create_job_args.get_default("use_fnapi") is True
                else "n",
                show_choices=False,
                show_default=False,  # shown in prompt
            )
        self.create_job_args.use_fnapi = use_fnapi

    def set_create_resources(self):
        create_resources = self.cmd_line_args.get("create_resources")
        if not create_resources:
            create_resources = click.prompt(
                "Create topics, buckets, and dashboards? [Y/n]",
                type=click.Choice(["y", "Y", "n", "N"]),
                default="n"
                if self.create_job_args.create_resources is False
                else "y",
                show_choices=False,
                show_default=False,  # shown in prompt
            )
        self.create_job_args.create_resources = create_resources

    def set_experiments(self):
        experiments = self.cmd_line_args.get("experiments")
        if not experiments:
            experiments = click.prompt(
                "Beam experiments to enable",
                default=",".join(self.create_job_args.experiments),
            )
        self.create_job_args.experiments = experiments

    def set_num_workers(self):
        num_workers = self.cmd_line_args.get("num_workers")
        if not num_workers:
            num_workers = click.prompt(
                "Number of workers to run",
                type=int,
                default=self.create_job_args.num_workers,
            )
        self.create_job_args.num_workers = num_workers

    def set_max_num_workers(self):
        max_num_workers = self.cmd_line_args.get("max_num_workers")
        if not max_num_workers:
            max_num_workers = click.prompt(
                "Maximum number of workers to run",
                type=int,
                default=self.create_job_args.max_num_workers,
            )
        self.create_job_args.max_num_workers = max_num_workers

    def set_autoscaling_algorithm(self):
        autoscaling_algorithm = self.cmd_line_args.get("autoscaling_algorithm")
        if not autoscaling_algorithm:
            autoscaling_algorithm = click.prompt(
                "Autoscaling algorithm to use. "
                "Can be NONE (default) or THROUGHPUT_BASED",
                type=str,
                default=self.create_job_args.autoscaling_algorithm,
            )
        self.create_job_args.autoscaling_algorithm = autoscaling_algorithm

    def set_disk_size_gb(self):
        disk_size_gb = self.cmd_line_args.get("disk_size_gb")
        if not disk_size_gb:
            disk_size_gb = click.prompt(
                "Size of a worker disk (GB)",
                type=int,
                default=self.create_job_args.disk_size_gb,
            )
        self.create_job_args.disk_size_gb = disk_size_gb

    def set_worker_machine_type(self):
        worker_machine_type = self.cmd_line_args.get("machine_type")
        if not worker_machine_type:
            worker_machine_type = click.prompt(
                "Type of GCP instance for the worker machine(s)",
                default=self.create_job_args.worker_machine_type,
            )
        self.create_job_args.worker_machine_type = worker_machine_type

    def set_worker_image_and_python_version(self):
        worker_image = self.cmd_line_args.get("worker_image")
        if not worker_image:
            worker_image = click.prompt(
                (
                    "Docker image to use for the worker."
                    " If none, a Dockerfile will"
                    " be created for you"
                ),
                default="",
            )
        if not worker_image:
            python_version = self.cmd_line_args.get("python_version")
            if not python_version:
                python_version = click.prompt(
                    "Python major version ({})".format(
                        ", ".join(VALID_BEAM_PY_VERSIONS)
                    ),
                    default=self.create_job_args.python_version,
                )
            self.create_job_args.python_version = python_version
        # post-refactor change in behavior: worker_image
        # is now set to the empty string if the user does not supply it
        # rather than being filled in with the default worker image value
        # this makes it possible to use this value later to decide
        # if a Dockerfile is created or not
        self.create_job_args.worker_image = worker_image

    def set_staging_location(self):
        staging_location = self.cmd_line_args.get("staging_location")
        if not staging_location:
            staging_location = click.prompt(
                "Staging environment location",
                default=self.create_job_args.staging_location,
            )
        self.create_job_args.staging_location = staging_location

    def set_temp_location(self):
        temp_location = self.cmd_line_args.get("temp_location")
        if not temp_location:
            temp_location = click.prompt(
                "Temporary environment location",
                default=self.create_job_args.temp_location,
            )
        self.create_job_args.temp_location = temp_location

    def set_input_topic(self):
        input_topic = self.cmd_line_args.get("input_topic")
        if not input_topic:
            input_topic = click.prompt(
                "Input topic (usually your dependency's output topic)",
                default=self.create_job_args.input_topic,
            )
        self.create_job_args.input_topic = input_topic

    def set_output_topic(self):
        output_topic = self.cmd_line_args.get("output_topic")
        if not output_topic:
            output_topic = click.prompt(
                "Output topic", default=self.create_job_args.output_topic
            )
        self.create_job_args.output_topic = output_topic

    def set_input_data_location(self):
        input_data_location = self.cmd_line_args.get("input_data_location")
        if not input_data_location:
            input_data_location = click.prompt(
                (
                    "Location of job's input data "
                    "(usually the location of your "
                    "dependency's output data)"
                ),
                default=self.create_job_args.input_data_location,
            )
        self.create_job_args.input_data_location = input_data_location

    def set_output_data_location(self):
        output_data_location = self.cmd_line_args.get("output_data_location")
        if not output_data_location:
            output_data_location = click.prompt(
                "Location of job's output",
                default=self.create_job_args.output_data_location,
            )
        self.create_job_args.output_data_location = output_data_location

    def set_subscription(self):
        self.create_job_args.subscription = self.create_job_args.get_default(
            "subscription"
        )

    def parse(self, create_job_args):
        self.create_job_args = create_job_args
        self.set_region()
        self.set_use_fnapi()
        self.set_create_resources()
        self.set_experiments()
        self.set_num_workers()
        self.set_max_num_workers()
        self.set_autoscaling_algorithm()
        self.set_disk_size_gb()
        self.set_worker_machine_type()
        self.set_worker_image_and_python_version()
        self.set_staging_location()
        self.set_temp_location()
        self.set_input_topic()
        self.set_subscription()
        self.set_output_topic()
        self.set_input_data_location()
        self.set_output_data_location()

        return create_job_args

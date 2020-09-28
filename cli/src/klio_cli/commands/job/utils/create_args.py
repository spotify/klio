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
            create_job_args.subscription = create_job_args.get_default("subscription")

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

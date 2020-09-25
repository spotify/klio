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

import attr
import click

# TODO: include all dataflow-supported GCP regions
VALID_BEAM_PY_VERSIONS = ["3.5", "3.6", "3.7"]
VALID_BEAM_PY_VERSIONS_SHORT = [
    "".join(p.split(".")) for p in VALID_BEAM_PY_VERSIONS
]
# TODO: include all dataflow-supported GCP regions
GCP_REGIONS = ["europe-west1", "us-central1", "asia-east1"]

def convert_to_bool(booleanish):
    if isinstance(booleanish, bool):
            return booleanish
    return booleanish.lower() in ["y", "true", "yes"]

def python_version_converter(python_version):
    # can't have something like 3.6.7.8
    if len(python_version) > 5:
        invalid_err_msg = (
            "Invalid Python version given: '{}'. Valid Python versions: "
            "{}".format(python_version, ", ".join(VALID_BEAM_PY_VERSIONS))
        )
        raise click.BadParameter(invalid_err_msg)

    # 3.x -> 3x; 3.x.y -> 3x
    if "." in python_version:
        python_version = "".join(python_version.split(".")[:2])

    # Dataflow's 3.5 image is just "python3" while 3.6 and 3.7 are
    # "python36" and "python37"
    if python_version == "35":
        python_version = "3"
    return python_version

@attr.s
class CreateJobArgs(object):
    # validation and conversion managed by click
    job_name = attr.ib()
    output = attr.ib()
    use_defaults = attr.ib()

    # validation
    worker_image = attr.ib(converter=attr.converters.optional(str))
    use_fnapi = attr.ib(converter=convert_to_bool)
    create_resources = attr.ib(
        converter=attr.converters.optional(convert_to_bool)
    )
    python_version = attr.ib(converter=attr.converters.optional(python_version_converter))

    # GCP specific
    gcp_project = attr.ib(converter=attr.converters.optional(str))
    staging_location = attr.ib(converter=attr.converters.optional(str))
    temp_location = attr.ib(converter=attr.converters.optional(str))
    output_topic = attr.ib(converter=attr.converters.optional(str))
    output_data_location = attr.ib(converter=attr.converters.optional(str))
    input_topic = attr.ib(converter=attr.converters.optional(str))
    input_data_location = attr.ib(converter=attr.converters.optional(str))
    subscription = attr.ib(converter=attr.converters.optional(str))
    experiments = attr.ib(converter=attr.converters.optional(list))
    region = attr.ib(converter=attr.converters.optional(str))
    num_workers = attr.ib(converter=attr.converters.optional(int))
    max_num_workers = attr.ib(converter=attr.converters.optional(int))
    autoscaling_algorithm = attr.ib(converter=attr.converters.optional(str))
    disk_size_gb = attr.ib(converter=attr.converters.optional(int))
    worker_machine_type = attr.ib(converter=attr.converters.optional(str))

    # derived
    create_dockerfile = attr.ib(converter=attr.converters.optional(bool))

    @classmethod
    def from_dict(cls, fields_dict):
        return cls(
            job_name=fields_dict.get("job_name"),
            output=fields_dict.get("output"),
            use_defaults=fields_dict.get("use_defaults"),
            worker_image=fields_dict.get("worker_image"),
            use_fnapi=fields_dict.get("use_fnapi"),
            create_resources=fields_dict.get("create_resources"),
            python_version=fields_dict.get("python_version"),
            gcp_project=fields_dict.get("gcp_project"),
            staging_location=fields_dict.get("staging_location"),
            temp_location=fields_dict.get("temp_location"),
            output_topic=fields_dict.get("output_topic"),
            output_data_location=fields_dict.get("output_data_location"),
            input_topic=fields_dict.get("input_topic"),
            input_data_location=fields_dict.get("input_data_location"),
            subscription=fields_dict.get("subscription"),
            experiments=fields_dict.get("experiments"),
            region=fields_dict.get("region"),
            num_workers=fields_dict.get("num_workers"),
            max_num_workers=fields_dict.get("max_num_workers"),
            autoscaling_algorithm=fields_dict.get("autoscaling_algorithm"),
            disk_size_gb=fields_dict.get("disk_size_gb"),
            worker_machine_type=fields_dict.get("worker_machine_type"),
            create_dockerfile=fields_dict.get("create_dockerfile"),
        )

    @python_version.validator
    def _parse_python_version(self, attr, python_version):
        if python_version.startswith("2"):
            msg = (
                "Klio no longer supports Python 2.7. "
                "Supported versions: {}".format(
                    ", ".join(VALID_BEAM_PY_VERSIONS)
                )
            )
            raise click.BadParameter(msg)

        invalid_err_msg = (
            "Invalid Python version given: '{}'. Valid Python versions: "
            "{}".format(python_version, ", ".join(VALID_BEAM_PY_VERSIONS))
        )

        # valid examples: 35, 3.5, 3.5.6
        if python_version not in VALID_BEAM_PY_VERSIONS_SHORT:
            raise click.BadParameter(invalid_err_msg)

    @worker_image.validator
    def _validate_worker_image(self, attr, value):
        # TODO: add validation that the image string is in the format of
        #       gcr.io/<project>/<name>(:<version>)  (@lynn)
        return

    @region.validator
    def _validate_region(self, attr, region):
        if region and region not in GCP_REGIONS:
            regions = ", ".join(GCP_REGIONS)
            msg = '"{0}" is not a valid region. Available: {1}'.format(
                region, regions
            )
            raise click.BadParameter(msg)
        return region



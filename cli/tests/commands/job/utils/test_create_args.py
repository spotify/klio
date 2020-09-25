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

import click
import mock
import pytest

from klio_cli.commands.job.utils import create_args

@pytest.fixture
def valid_dict():
    return {
        "job_name": "job_name",
        "output": ".",
        "use_defaults": False,
        "worker_image": "worker_image",
        "use_fnapi": False,
        "create_resources": "create_resources",
        "python_version": "3.6",
        "gcp_project": "gcp_project",
        "staging_location": "staging_location",
        "temp_location": "temp_location",
        "output_topic": "output_topic",
        "output_data_location": "output_data_location",
        "input_topic": "input_topic",
        "input_data_location": "input_data_location",
        "subscription": "subscription",
        "experiments": "experiments",
        "region": None,
        "num_workers": "10",
        "max_num_workers": "30",
        "autoscaling_algorithm": "autoscaling_algorithm",
        "disk_size_gb": "10000",
        "worker_machine_type": "worker_machine_type",
        "create_dockerfile": False,
    }

def test_from_dict(valid_dict):
    expected_args = create_args.CreateJobArgs(
        job_name=valid_dict.get("job_name"),
        output=valid_dict.get("output"),
        use_defaults=valid_dict.get("use_defaults"),
        worker_image=valid_dict.get("worker_image"),
        use_fnapi=valid_dict.get("use_fnapi"),
        create_resources=valid_dict.get("create_resources"),
        python_version=valid_dict.get("python_version"),
        gcp_project=valid_dict.get("gcp_project"),
        staging_location=valid_dict.get("staging_location"),
        temp_location=valid_dict.get("temp_location"),
        output_topic=valid_dict.get("output_topic"),
        output_data_location=valid_dict.get("output_data_location"),
        input_topic=valid_dict.get("input_topic"),
        input_data_location=valid_dict.get("input_data_location"),
        subscription=valid_dict.get("subscription"),
        experiments=valid_dict.get("experiments"),
        region=valid_dict.get("region"),
        num_workers=valid_dict.get("num_workers"),
        max_num_workers=valid_dict.get("max_num_workers"),
        autoscaling_algorithm=valid_dict.get("autoscaling_algorithm"),
        disk_size_gb=valid_dict.get("disk_size_gb"),
        worker_machine_type=valid_dict.get("worker_machine_type"),
        create_dockerfile=valid_dict.get("create_dockerfile"),
    )
    assert expected_args == create_args.CreateJobArgs.from_dict(valid_dict)

@pytest.mark.parametrize(
    "input_version,exp_output_version",
    (
            ("3.5", "3"),
            ("3.5.1", "3"),
            ("35", "3"),
            ("3.6", "36"),
            ("3.6.1", "36"),
            ("36", "36"),
            ("3.7", "37"),
            ("3.7.1", "37"),
            ("37", "37"),
    ),
)
def test_python_version_converter(input_version, exp_output_version):
    assert exp_output_version == create_args.python_version_converter(input_version)

def test_python_version_converter_raises():
    input_version = "3.6.7.8"
    exp_msg = "Invalid Python version given"
    with pytest.raises(click.BadParameter, match=exp_msg):
        create_args.python_version_converter(input_version)

@pytest.mark.parametrize(
    "input_version,exp_msg",
    (
            ("2", "Klio no longer supports Python 2.7"),
            ("2.7", "Klio no longer supports Python 2.7"),
            ("3", "Invalid Python version given"),
            ("3.3", "Invalid Python version given"),
    ),
)
def test_python_version_validator_raises(input_version, exp_msg, valid_dict):
    valid_dict["python_version"] = input_version
    # only matching the start of the error message
    with pytest.raises(click.BadParameter, match=exp_msg):
        create_args.CreateJobArgs.from_dict(valid_dict)

@pytest.mark.parametrize("valid_region", ("us-central1", None))
def test_validate_region(valid_dict, valid_region):
    valid_dict["region"] = valid_region
    create_args_inst = create_args.CreateJobArgs.from_dict(valid_dict)
    assert valid_region == create_args_inst.region


def test_validate_region_raises(valid_dict):
    valid_dict["region"] = "not-a-region"
    with pytest.raises(click.BadParameter) as e:
        create_args.CreateJobArgs.from_dict(valid_dict)

    assert e.match(
        '"{}" is not a valid region. Available: '.format(valid_dict["region"])
    )


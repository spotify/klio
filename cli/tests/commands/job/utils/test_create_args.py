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
import pytest

from klio_cli.commands.job.utils import create_args


@pytest.mark.parametrize(
    "booleanish, expected",
    (
        ("y", True),
        ("Y", True),
        ("yes", True),
        ("YES", True),
        ("true", True),
        ("TRUE", True),
        (True, True),
        (False, False),
        ("no", False),
        ("nononononon", False),
        ("FALSE", False),
        ("spaghetti", False),
    ),
)
def test_convert_to_bool(booleanish, expected):
    assert expected == create_args.convert_to_bool(booleanish)


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
    assert exp_output_version == create_args.python_version_converter(
        input_version
    )


@pytest.mark.parametrize(
    "value, expected",
    (
        ("something", ["something"]),
        ("something,something", ["something", "something"]),
        (["something"], ["something"]),
    ),
)
def test_experiments_converter(value, expected):
    assert expected == create_args.experiments_converter(value)


@pytest.fixture
def valid_dict():
    return {
        "job_name": "job_name",
        "worker_image": "worker_image",
        "create_resources": "create_resources",
        "python_version": "3.6",
        "gcp_project": "gcp-project",
        "staging_location": "staging_location",
        "temp_location": "temp_location",
        "output_topic": "output_topic",
        "output_data_location": "output_data_location",
        "input_topic": "input_topic",
        "input_data_location": "input_data_location",
        "subscription": "subscription",
        "use_fnapi": True,
        "experiments": "experiments",
        "num_workers": "10",
        "max_num_workers": "30",
        "autoscaling_algorithm": "autoscaling_algorithm",
        "disk_size_gb": "10000",
        "worker_machine_type": "worker_machine_type",
    }


def test_from_dict(valid_dict):
    expected_args = create_args.CreateJobArgs(
        job_name=valid_dict.get("job_name"),
        worker_image=valid_dict.get("worker_image"),
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
        use_fnapi=valid_dict.get("use_fnapi"),
        experiments=valid_dict.get("experiments"),
        region=valid_dict.get("region"),
        num_workers=valid_dict.get("num_workers"),
        max_num_workers=valid_dict.get("max_num_workers"),
        autoscaling_algorithm=valid_dict.get("autoscaling_algorithm"),
        disk_size_gb=valid_dict.get("disk_size_gb"),
        worker_machine_type=valid_dict.get("worker_machine_type"),
    )
    expected_args.region = (
        "europe-west1"  # since region not provided, expecting default
    )
    assert expected_args == create_args.CreateJobArgs.from_dict(valid_dict)


@pytest.mark.parametrize(
    "input_version,exp_msg",
    (
        ("2", "Klio no longer supports Python 2.7"),
        ("27", "Klio no longer supports Python 2.7"),
        ("3", "Invalid Python version given"),
        ("33", "Invalid Python version given"),
        ("3678", "Invalid Python version given"),
        ("blackmamba", "Invalid Python version given"),
    ),
)
def test_python_version_validator_raises(input_version, exp_msg, valid_dict):
    valid_dict["python_version"] = input_version
    # only matching the start of the error message
    with pytest.raises(
        create_args.CreateJobArgsValidationError, match=exp_msg
    ):
        create_args.CreateJobArgs.from_dict(valid_dict)


def test_validate_region_raises(valid_dict):
    valid_dict["region"] = "not-a-region"
    with pytest.raises(create_args.CreateJobArgsValidationError) as e:
        create_args.CreateJobArgs.from_dict(valid_dict)

    assert e.match(
        '"{}" is not a valid region. Available: '.format(valid_dict["region"])
    )


@pytest.fixture
def create_job_args_default_inst():
    return create_args.CreateJobArgs(gcp_project="stinky", job_name="cheese")


def test_default_worker_image(create_job_args_default_inst):
    expected = "gcr.io/stinky/cheese-worker"
    assert expected == create_job_args_default_inst.worker_image


def test_default_bucket(create_job_args_default_inst):
    expected = "gs://stinky-dataflow-tmp/cheese"
    assert expected == create_job_args_default_inst._default_bucket


def test_default_temp_location(create_job_args_default_inst):
    expected = "gs://stinky-dataflow-tmp/cheese/temp"
    assert expected == create_job_args_default_inst.temp_location


def test_default_staging_location(create_job_args_default_inst):
    expected = "gs://stinky-dataflow-tmp/cheese/staging"
    assert expected == create_job_args_default_inst.staging_location


def test_default_base_topic(create_job_args_default_inst):
    expected = "projects/stinky/topics/cheese"
    assert expected == create_job_args_default_inst._default_base_topic


def test_default_output_topic(create_job_args_default_inst):
    expected = "projects/stinky/topics/cheese-output"
    assert expected == create_job_args_default_inst.output_topic


def test_default_input_topic(create_job_args_default_inst):
    expected = "projects/stinky/topics/cheese-input"
    assert expected == create_job_args_default_inst.input_topic


def test_default_input_subscription(create_job_args_default_inst):
    expected = "projects/stinky/subscriptions/cheese-input-cheese"
    assert expected == create_job_args_default_inst.subscription


def test_default_output_data_location(create_job_args_default_inst):
    expected = "gs://stinky-output/cheese"
    assert expected == create_job_args_default_inst.output_data_location


def test_default_input_data_location(create_job_args_default_inst):
    expected = "gs://stinky-input/cheese"
    assert expected == create_job_args_default_inst.input_data_location


def test_get_default(create_job_args_default_inst):
    for field in attr.fields_dict(create_args.CreateJobArgs):
        if field not in ("job_name", "gcp_project"):
            assert getattr(
                create_job_args_default_inst, field
            ) == create_job_args_default_inst.get_default(field)

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
import pytest

from klio_cli.commands.job.utils import create_args


@pytest.fixture
def mock_prompt(mocker):
    return mocker.patch.object(create_args.click, "prompt")


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_region(mock_prompt, source):
    command_line_args = {}
    expected = "us-central1"
    if source == "command_line":
        command_line_args["region"] = expected
    elif source == "prompt":
        mock_prompt.side_effect = [expected]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_region()

    assert expected == j.create_job_args.region

    if source == "prompt":
        assert mock_prompt.called_once_with(
            "Desired GCP Region", default=create_job_args.get_default("region")
        )


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_use_fnapi(mock_prompt, source):
    command_line_args = {}
    expected = False
    if source == "command_line":
        command_line_args["use_fnapi"] = "false"
    elif source == "prompt":
        mock_prompt.side_effect = ["n"]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_use_fnapi()

    assert expected == j.create_job_args.use_fnapi

    if source == "prompt":
        expected_kwargs = {
            "text": "Use Apache Beam's FnAPI (experimental) [Y/n]",
            "type": click.Choice(["y", "Y", "n", "N"]),
            "default": "y"
            if create_job_args.get_default("use_fnapi") is True
            else "n",
            "show_choices": False,
            "show_default": False,  # shown in prompt
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_create_resources(mock_prompt, source):
    command_line_args = {}
    expected = False
    if source == "command_line":
        command_line_args["create_resources"] = "false"
    elif source == "prompt":
        mock_prompt.side_effect = ["n"]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_create_resources()

    assert expected == j.create_job_args.create_resources

    if source == "prompt":
        expected_kwargs = {
            "text": "Create topics, buckets, and dashboards? [Y/n]",
            "type": click.Choice(["y", "Y", "n", "N"]),
            "default": "n"
            if create_job_args.create_resources is False
            else "y",
            "show_choices": False,
            "show_default": False,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_experiments(mock_prompt, source):
    command_line_args = {}
    expected = ["another-experiment"]
    if source == "command_line":
        command_line_args["experiments"] = expected
    elif source == "prompt":
        mock_prompt.side_effect = expected

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_experiments()

    assert expected == j.create_job_args.experiments

    if source == "prompt":
        expected_kwargs = {
            "text": "Beam experiments to enable",
            "default": ",".join(create_job_args.experiments),
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_num_workers(mock_prompt, source):
    command_line_args = {}
    expected = 2
    if source == "command_line":
        command_line_args["num_workers"] = str(expected)
    elif source == "prompt":
        mock_prompt.side_effect = [str(expected)]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_num_workers()

    assert expected == j.create_job_args.num_workers

    if source == "prompt":
        expected_kwargs = {
            "text": "Number of workers to run",
            "type": int,
            "default": create_job_args.num_workers,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_max_num_workers(mock_prompt, source):
    command_line_args = {}
    expected = 2
    if source == "command_line":
        command_line_args["max_num_workers"] = str(expected)
    elif source == "prompt":
        mock_prompt.side_effect = [str(expected)]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_max_num_workers()

    assert expected == j.create_job_args.max_num_workers

    if source == "prompt":
        expected_kwargs = {
            "text": "Number of workers to run",
            "type": int,
            "default": create_job_args.max_num_workers,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_autoscaling_algorithm(mock_prompt, source):
    command_line_args = {}
    expected = "NONE"
    if source == "command_line":
        command_line_args["autoscaling_algorithm"] = expected
    elif source == "prompt":
        mock_prompt.side_effect = [expected]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_autoscaling_algorithm()

    assert expected == j.create_job_args.autoscaling_algorithm

    if source == "prompt":
        expected_kwargs = {
            "text": "Autoscaling algorithm to use. "
            "Can be NONE (default) or THROUGHPUT_BASED",
            "type": str,
            "default": create_job_args.autoscaling_algorithm,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_disk_size_gb(mock_prompt, source):
    command_line_args = {}
    expected = 10
    if source == "command_line":
        command_line_args["disk_size_gb"] = str(expected)
    elif source == "prompt":
        mock_prompt.side_effect = [str(expected)]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_disk_size_gb()

    assert expected == j.create_job_args.disk_size_gb

    if source == "prompt":
        expected_kwargs = {
            "text": "Size of a worker disk (GB)",
            "type": int,
            "default": create_job_args.disk_size_gb,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
def test_set_worker_machine_type(mock_prompt, source):
    command_line_args = {}
    expected = "ns-standard-1"
    if source == "command_line":
        command_line_args["machine_type"] = expected
    elif source == "prompt":
        mock_prompt.side_effect = [expected]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_worker_machine_type()

    assert expected == j.create_job_args.worker_machine_type

    if source == "prompt":
        expected_kwargs = {
            "text": "Type of GCP instance for the worker machine(s)",
            "default": create_job_args.worker_machine_type,
        }
        assert mock_prompt.called_once_with(**expected_kwargs)


@pytest.mark.parametrize("source", ("command_line", "prompt"))
@pytest.mark.parametrize(
    "setter_func_name, field_name, expected_value, expected_prompt",
    (
        # region setter test
        (
            "set_region",
            "region",
            "us-central1",
            {"text": "Desired GCP Region", "default": "europe-west1"},
        ),
        (
            "set_use_fnapi",
            "use_fnapi",
            False,
            {
                "text": "Use Apache Beam's FnAPI (experimental) [Y/n]",
                "type": click.Choice(["y", "Y", "n", "N"]),
                "default": "n",
                "show_choices": False,
                "show_default": False,  # shown in prompt
            },
        ),
        (
            "set_create_resources",
            "create_resources",
            False,
            {
                "text": "Create topics, buckets, and dashboards? [Y/n]",
                "type": click.Choice(["y", "Y", "n", "N"]),
                "default": "n",
                "show_choices": False,
                "show_default": False,
            },
        ),
        (
            "set_experiments",
            "experiments",
            ["beam_fn_api"],
            {"text": "Beam experiments to enable", "default": "beam_fn_api"},
        ),
        (
            "set_num_workers",
            "num_workers",
            2,
            {"text": "Number of workers to run", "type": int, "default": 2},
        ),
        (
            "set_max_num_workers",
            "max_num_workers",
            2,
            {"text": "Number of workers to run", "type": int, "default": 2},
        ),
        (
            "set_autoscaling_algorithm",
            "autoscaling_algorithm",
            "NONE",
            {
                "text": "Autoscaling algorithm to use. "
                "Can be NONE (default) or THROUGHPUT_BASED",
                "type": str,
                "default": "NONE",
            },
        ),
        (
            "set_disk_size_gb",
            "disk_size_gb",
            32,
            {
                "text": "Size of a worker disk (GB)",
                "type": int,
                "default": 32,
            },
        ),
        (
            "set_worker_machine_type",
            "worker_machine_type",
            "n1-highmem-32",
            {
                "text": "Type of GCP instance for the worker machine(s)",
                "default": "n1-standard-2",
            },
        ),
        (
            "set_staging_location",
            "staging_location",
            "my/staging/location",
            {
                "text": "Staging environment location",
                "default": "gs://test-gcp-project-dataflow-tmp/test-job",
            },
        ),
        (
            "set_temp_location",
            "temp_location",
            "my/temp/location",
            {
                "text": "Temporary environment location",
                "default": "gs://test-gcp-project-dataflow-tmp/test-job/temp",
            },
        ),
        (
            "set_input_topic",
            "input_topic",
            "my-input-topic",
            {
                "text": "Input topic (usually your dependency's output topic)",
                "default": "projects/test-gcp-project/topics/test-job-input",
            },
        ),
        (
            "set_output_topic",
            "output_topic",
            "my-output-topic",
            {
                "text": "Output topic",
                "default": "projects/test-gcp-project/topics/test-job-output",
            },
        ),
        (
            "set_input_data_location",
            "input_data_location",
            "my/input/data",
            {
                "text": "Location of job's input data "
                "(usually the location of your "
                "dependency's output data)",
                "default": "gs://test-gcp-project-input/test-job",
            },
        ),
        (
            "set_output_data_location",
            "output_data_location",
            "my/output/data",
            {
                "text": "Location of job's output",
                "default": "gs://test-gcp-project-output/test-job",
            },
        ),
    ),
)
def test_setters(
    mock_prompt,
    setter_func_name,
    field_name,
    source,
    expected_value,
    expected_prompt,
):
    command_line_args = {}

    value = expected_value
    if isinstance(expected_value, list):
        value = ",".join(expected_value)

    if source == "command_line":
        if field_name == "worker_machine_type":
            command_line_args["machine_type"] = str(value)
        else:
            command_line_args[field_name] = str(value)
    elif source == "prompt":
        mock_prompt.side_effect = [str(value)]

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args

    # run the setter function
    field_setter_func = getattr(j, setter_func_name)
    field_setter_func()

    # check value set
    assert expected_value == getattr(j.create_job_args, field_name)

    if source == "prompt":
        assert mock_prompt.called_once_with(**expected_prompt)


@pytest.mark.parametrize("worker_image_provided_by_prompt", (True, False))
@pytest.mark.parametrize("python_version_source", ("command_line", "prompt"))
@pytest.mark.parametrize("worker_image_source", ("command_line", "prompt"))
def test_set_worker_image_and_python_version(
    mocker,
    mock_prompt,
    worker_image_provided_by_prompt,
    worker_image_source,
    python_version_source,
):
    command_line_args = {}
    side_effects = []
    expected_python_version = "37"

    expected_worker_image = "a-worker-image"
    if worker_image_source == "prompt":
        if not worker_image_provided_by_prompt:
            expected_worker_image = ""
        side_effects.append(expected_worker_image)
    elif worker_image_source == "command_line":
        command_line_args["worker_image"] = expected_worker_image

    if not expected_worker_image:
        if python_version_source == "prompt":
            side_effects.append(expected_python_version)
        if python_version_source == "command_line":
            command_line_args["python_version"] = expected_python_version
    else:
        expected_python_version = "36"  # default version
    mock_prompt.side_effect = side_effects

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    j.create_job_args = create_job_args
    j.set_worker_image_and_python_version()

    assert expected_worker_image == j.create_job_args.worker_image
    assert expected_python_version == j.create_job_args.python_version

    expected_worker_image_call = mocker.call(
        "Docker image to use for the worker. "
        "If none, a Dockerfile will be created for you",
        default="",
    )
    expected_python_version_call = mocker.call(
        "Python major version ({})".format(
            ", ".join(create_args.VALID_BEAM_PY_VERSIONS)
        ),
        default="36",
    )
    if (
        worker_image_source == "prompt"
        and not worker_image_provided_by_prompt
        and python_version_source == "prompt"
    ):
        mock_prompt.assert_has_calls(
            [expected_worker_image_call, expected_python_version_call]
        )
    elif worker_image_source == "prompt":
        mock_prompt.assert_has_calls([expected_worker_image_call])


def test_set_subscription(mocker):
    command_line_args = {}
    j = create_args.CreateJobPromptInput(command_line_args)
    j.create_job_args = mocker.Mock()
    mock_get_default = mocker.patch.object(j.create_job_args, "get_default")
    j.set_subscription()

    mock_get_default.assert_called_once_with("subscription")


def test_parse(mocker):
    command_line_args = {}

    j = create_args.CreateJobPromptInput(command_line_args)
    create_job_args = create_args.CreateJobArgs(
        job_name="test-job", gcp_project="test-gcp-project"
    )
    mock_methods = [
        mocker.patch.object(j, "set_region"),
        mocker.patch.object(j, "set_use_fnapi"),
        mocker.patch.object(j, "set_create_resources"),
        mocker.patch.object(j, "set_experiments"),
        mocker.patch.object(j, "set_num_workers"),
        mocker.patch.object(j, "set_max_num_workers"),
        mocker.patch.object(j, "set_autoscaling_algorithm"),
        mocker.patch.object(j, "set_disk_size_gb"),
        mocker.patch.object(j, "set_worker_machine_type"),
        mocker.patch.object(j, "set_worker_image_and_python_version"),
        mocker.patch.object(j, "set_staging_location"),
        mocker.patch.object(j, "set_temp_location"),
        mocker.patch.object(j, "set_input_topic"),
        mocker.patch.object(j, "set_subscription"),
        mocker.patch.object(j, "set_output_topic"),
        mocker.patch.object(j, "set_input_data_location"),
        mocker.patch.object(j, "set_output_data_location"),
    ]
    j.parse(create_job_args)

    for m in mock_methods:
        m.assert_called_once_with()

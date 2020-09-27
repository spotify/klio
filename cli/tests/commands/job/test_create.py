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

import os

import click
import pytest

from klio_cli.commands.job import create
from klio_cli.commands.job.utils import create_args

HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "utils", "fixtures")


@pytest.fixture
def job():
    return create.CreateJob()


@pytest.fixture
def context():
    base_gcs = "gs://test-gcp-project-dataflow-tmp/test-job"
    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    return {
        "job_name": "test-job",
        "python_version": "36",
        "pipeline_options": {
            "project": "test-gcp-project",
            "region": "europe-west1",
            "worker_harness_container_image": gcr_url,
            "experiments": ["beam_fn_api"],
            "staging_location": base_gcs + "/staging",
            "temp_location": base_gcs + "/temp",
            "num_workers": 2,
            "max_num_workers": 2,
            "autoscaling_algorithm": "NONE",
            "disk_size_gb": 32,
            "worker_machine_type": "n1-standard-2",
        },
        "job_options": {
            "inputs": [
                {
                    "topic": (
                        "projects/test-parent-gcp-project/topics/"
                        "test-parent-job-output"
                    ),
                    "subscription": (
                        "projects/test-gcp-project/subscriptions/"
                        "test-parent-job-output-test-job"
                    ),
                    "data_location": (
                        "gs://test-parent-gcp-project-output/test-parent-job"
                    ),
                }
            ],
            "outputs": [
                {
                    "topic": "projects/test-gcp-project/topics/test-job-output",
                    "data_location": "gs://test-gcp-project-output/test-job",
                }
            ],
        },
    }


@pytest.fixture
def default_context():
    base_gcs = "gs://test-gcp-project-dataflow-tmp/test-job"
    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    return {
        "job_name": "test-job",
        "python_version": "36",
        "use_fnapi": True,
        "create_resources": False,
        "pipeline_options": {
            "project": "test-gcp-project",
            "region": "europe-west1",
            "worker_harness_container_image": gcr_url,
            "experiments": ["beam_fn_api"],
            "staging_location": base_gcs + "/staging",
            "temp_location": base_gcs + "/temp",
            "num_workers": 2,
            "max_num_workers": 2,
            "autoscaling_algorithm": "NONE",
            "disk_size_gb": 32,
            "worker_machine_type": "n1-standard-2",
        },
        "job_options": {
            "inputs": [
                {
                    "topic": (
                        "projects/test-gcp-project/topics/test-job-input"
                    ),
                    "subscription": (
                        "projects/test-gcp-project/subscriptions/"
                        "test-job-input-test-job"
                    ),
                    "data_location": ("gs://test-gcp-project-input/test-job"),
                }
            ],
            "outputs": [
                {
                    "topic": "projects/test-gcp-project/topics/test-job-output",
                    "data_location": "gs://test-gcp-project-output/test-job",
                }
            ],
        },
    }


def test_validate_worker_image(job):
    assert not job._validate_worker_image("foo")


def test_validate_region(job):
    exp_region = "us-central1"
    ret_region = job._validate_region(exp_region)
    assert exp_region == ret_region


def test_validate_region_raises(job):
    err_region = "not-a-region"
    with pytest.raises(click.BadParameter) as e:
        job._validate_region(err_region)

    assert e.match(
        '"{}" is not a valid region. Available: '.format(err_region)
    )


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
def test_parse_python_version(input_version, exp_output_version, job):
    assert exp_output_version == job._parse_python_version(input_version)


@pytest.mark.parametrize(
    "input_version,exp_msg",
    (
        ("2", "Klio no longer supports Python 2.7"),
        ("2.7", "Klio no longer supports Python 2.7"),
        ("3", "Invalid Python version given"),
        ("3.3", "Invalid Python version given"),
        ("3.6.7.8", "Invalid Python version given"),
    ),
)
def test_parse_python_version_raises(input_version, exp_msg, job):
    # only matching the start of the error message
    with pytest.raises(click.BadParameter, match=exp_msg):
        job._parse_python_version(input_version)


def test_get_context_from_defaults(default_context, job):
    basic_context = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
    }

    ret_context, ret_create_dockerfile = job._get_context_from_defaults(
        basic_context
    )
    default_context.pop("job_name")

    assert default_context == ret_context
    assert ret_create_dockerfile


@pytest.fixture
def context_overrides():
    return {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
        "worker_image": "gcr.io/foo/bar",
        "experiments": "beam_fn_api,another_experiment",
        "region": "us-central1",
        "staging_location": "gs://a-different/staging/location",
        "temp_location": "gs://a-different/temp/location",
        "num_workers": 1000000,
        "max_num_workers": 1000000,
        "autoscaling_algorithm": "THROUGHPUT_BASED",
        "disk_size_gb": 1000000,
        "input_topic": "projects/test-gcp-projects/topics/another-topic",
        "output_topic": "a-different-topic",
        "input_data_location": "gs://test-parent-gcp-project/test-parent-job",
        "output_data_location": "bq://somewhere/over/the/rainbow",
        "python_version": "37",
        "use_fnapi": "n",
        "create_resources": "n",
    }


@pytest.fixture
def expected_overrides():
    return {
        "pipeline_options": {
            "project": "test-gcp-project",
            "worker_harness_container_image": "gcr.io/foo/bar",
            "experiments": ["beam_fn_api", "another_experiment"],
            "region": "us-central1",
            "staging_location": "gs://a-different/staging/location",
            "temp_location": "gs://a-different/temp/location",
            "num_workers": 1000000,
            "max_num_workers": 1000000,
            "autoscaling_algorithm": "THROUGHPUT_BASED",
            "disk_size_gb": 1000000,
            "worker_machine_type": "n4-highmem-l33t",
        },
        "job_options": {
            "inputs": [
                {
                    "topic": "projects/test-gcp-projects/topics/another-topic",
                    "subscription": (
                        "projects/test-gcp-project/subscriptions/"
                        "another-topic-test-job"
                    ),
                    "data_location": (
                        "gs://test-parent-gcp-project/test-parent-job"
                    ),
                }
            ],
            "outputs": [
                {
                    "topic": "a-different-topic",
                    "data_location": "bq://somewhere/over/the/rainbow",
                }
            ],
        },
        "python_version": "37",
        "use_fnapi": False,
        "create_resources": False,
    }


def test_get_context_from_defaults_overrides(
    context_overrides, expected_overrides, job
):
    # FYI: Click will pass in kwargs as a flat dict
    context_overrides["worker_machine_type"] = "n4-highmem-l33t"
    ret_context, ret_create_dockerfile = job._get_context_from_defaults(
        context_overrides
    )

    assert expected_overrides == ret_context
    assert not ret_create_dockerfile


@pytest.fixture
def mock_prompt(mocker):
    return mocker.patch.object(create.click, "prompt")


def test_get_context_from_user_inputs(
    context, mock_prompt, mocker, job,
):
    # mimicking user inputs for each prompt
    prompt_side_effect = [
        "europe-west1",
        "Y",
        "n",
        ["beam_fn_api"],
        2,
        2,
        "NONE",
        32,
        "n1-standard-2",
        "",
        "36",
        "gs://test-gcp-project-dataflow-tmp/test-job/staging",
        "gs://test-gcp-project-dataflow-tmp/test-job/temp",
        "projects/test-parent-gcp-project/topics/test-parent-job-output",
        "projects/test-gcp-project/topics/test-job-output",
        "gs://test-parent-gcp-project-output/test-parent-job",
        "gs://test-gcp-project-output/test-job",
    ]
    mock_prompt.side_effect = prompt_side_effect

    user_input_context = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
    }

    mock_validate_region = mocker.patch.object(job, "_validate_region")
    mock_validate_worker_image = mocker.patch.object(
        job, "_validate_worker_image"
    )
    ret_context, ret_dockerfile = job._get_context_from_user_inputs(
        user_input_context
    )

    assert len(prompt_side_effect) == mock_prompt.call_count

    exp_calls = [mocker.call("europe-west1")]
    assert exp_calls == mock_validate_region.call_args_list
    assert len(exp_calls) == mock_validate_region.call_count

    # mock_validate_region.assert_called_once_with("europe-west1")
    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    mock_validate_worker_image.assert_called_once_with(gcr_url)

    context.pop("job_name")
    context["pipeline_options"].pop("project")
    context["use_fnapi"] = True
    context["create_resources"] = False
    assert context == ret_context
    assert ret_dockerfile


def test_get_context_from_user_inputs_no_prompts(
    mocker, context_overrides, expected_overrides, mock_prompt, job,
):
    context_overrides["machine_type"] = "n4-highmem-l33t"
    mock_validate_region = mocker.patch.object(job, "_validate_region")
    mock_validate_worker_image = mocker.patch.object(
        job, "_validate_worker_image"
    )
    ret_context, ret_dockerfile = job._get_context_from_user_inputs(
        context_overrides
    )

    expected_overrides["pipeline_options"].pop("project")
    expected_overrides["python_version"] = "36"
    assert not mock_prompt.call_count
    mock_validate_region.assert_called_once_with("us-central1")
    mock_validate_worker_image.assert_called_once_with("gcr.io/foo/bar")
    assert not ret_dockerfile
    assert expected_overrides == ret_context


def test_get_context_from_user_inputs_no_prompts_image(
    mocker, context_overrides, expected_overrides, mock_prompt, job,
):
    mock_prompt.side_effect = [""]

    context_overrides.pop("worker_image")
    context_overrides["machine_type"] = "n4-highmem-l33t"

    mock_validate_region = mocker.patch.object(job, "_validate_region")
    mock_validate_worker_image = mocker.patch.object(
        job, "_validate_worker_image"
    )
    ret_context, ret_dockerfile = job._get_context_from_user_inputs(
        context_overrides
    )

    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    exp_pipeline_opts = expected_overrides["pipeline_options"]
    exp_pipeline_opts.pop("project")
    exp_pipeline_opts["worker_harness_container_image"] = gcr_url

    assert 1 == mock_prompt.call_count
    mock_validate_region.assert_called_once_with("us-central1")
    mock_validate_worker_image.assert_called_once_with(gcr_url)
    assert ret_dockerfile
    assert expected_overrides == ret_context


@pytest.mark.parametrize("use_defaults", (True, False))
def test_get_user_input(use_defaults, mocker, job):
    ret_context = {"pipeline_options": {}}
    mock_get_context_defaults = mocker.patch.object(
        job, "_get_context_from_defaults"
    )
    mock_get_context_defaults.return_value = (ret_context, True)
    mock_get_context_user = mocker.patch.object(
        job, "_get_context_from_user_inputs"
    )
    mock_get_context_user.return_value = (ret_context, True)

    input_kwargs = {
        "use_defaults": use_defaults,
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
    }

    job._get_user_input(input_kwargs)
    if use_defaults:
        mock_get_context_defaults.assert_called_once_with(input_kwargs)
    else:
        mock_get_context_user.assert_called_once_with(input_kwargs)


@pytest.mark.parametrize(
    "unknown_args,expected",
    (
        (("--foo", "foobar"), {"foo": "foobar"}),
        (
            ("--foo", "foobar", "--bar", "barfoo"),
            {"foo": "foobar", "bar": "barfoo"},
        ),
        (("--foo", "bar", "baz"), {"foo": ("bar", "baz")}),
    ),
)
def test_parse_unknown_args(unknown_args, expected, job):
    ret_args = job._parse_unknown_args(unknown_args)
    assert expected == ret_args


@pytest.mark.parametrize("create_dockerfile", (True, False))
@pytest.mark.parametrize("use_fnapi", (True, False))
@pytest.mark.parametrize("create_resources", (True, False))
def test_create(
    use_fnapi, create_dockerfile, create_resources, mocker, caplog, job
):
    context = {
        "job_name": "test-job",
        "use_fnapi": use_fnapi,
        "create_resources": create_resources,
    }

    mock_get_user_input = mocker.patch.object(job, "_get_user_input")
    mock_get_user_input.return_value = (context, create_dockerfile)
    template_renderer = mocker.Mock()
    template_renderer_const = mocker.patch.object(
        create.rendering, "CreateJobTemplateRenderer"
    )
    template_renderer_const.return_value = template_renderer
    mock_get_environment = mocker.patch.object(
        template_renderer, "get_environment"
    )
    mock_create_job_dir = mocker.patch.object(
        template_renderer, "create_job_directory"
    )
    mock_create_job_config = mocker.patch.object(
        template_renderer, "create_job_config"

    )
    mock_create_no_fnapi_files = mocker.patch.object(
        template_renderer, "create_no_fnapi_files"
    )
    mock_create_python_files = mocker.patch.object(
        template_renderer, "create_python_files"
    )
    mock_create_reqs_files = mocker.patch.object(
        template_renderer, "create_reqs_file"
    )
    mock_create_dockerfile = mocker.patch.object(
        template_renderer, "create_dockerfile"
    )
    mock_create_readme = mocker.patch.object(
        template_renderer, "create_readme"
    )
    mock_create_topics = mocker.patch.object(
        create.gcp_setup, "create_topics_and_buckets"
    )
    mock_create_stackdriver = mocker.patch.object(
        create.gcp_setup, "create_stackdriver_dashboard"
    )

    unknown_args = ("--foo", "bar")
    known_args = {
        "job_name": "test-job",
        "gcp-project": "test-gcp-project",
    }
    output_dir = "/testing/dir"

    job.create(unknown_args, known_args, output_dir)

    known_args["foo"] = "bar"
    mock_get_user_input.assert_called_once_with(known_args)

    mock_get_environment.assert_called_once_with()

    ret_env = mock_get_environment.return_value
    job_name = context["job_name"]
    package_name = job_name.replace("-", "_")
    mock_create_job_dir.assert_called_once_with(output_dir)

    mock_create_job_config.assert_called_once_with(
        ret_env, context, output_dir
    )

    mock_create_python_files.assert_called_once_with(
        ret_env, package_name, output_dir
    )
    if use_fnapi:
        mock_create_no_fnapi_files.assert_not_called()
    else:
        mock_create_no_fnapi_files.assert_called_once_with(
            ret_env, context, output_dir
        )

    if create_resources:
        mock_create_topics.assert_called_once_with(context)
        mock_create_stackdriver.assert_called_once_with(context)
    else:
        mock_create_topics.assert_not_called()
        mock_create_stackdriver.assert_not_called()

    mock_create_reqs_files.assert_called_once_with(
        ret_env, context, output_dir
    )
    if create_dockerfile:
        mock_create_dockerfile.assert_called_once_with(
            ret_env, context, output_dir
        )
    mock_create_readme.assert_called_once_with(ret_env, context, output_dir)
    assert 1 == len(caplog.records)


@pytest.mark.parametrize("user_prompt_worker_image", ("", "my-worker"))
def test_create_args_from_user_prompt(
    job, mock_prompt, user_prompt_worker_image
):
    expected_create_job_args = create_args.CreateJobArgs(
        job_name="test-job",
        worker_image="gcr.io/test-gcp-project/test-job-worker",
        create_resources=False,
        python_version="36",
        gcp_project="test-gcp-project",
        staging_location="my-staging-location",
        temp_location="my-temp-location",
        output_topic="my-output-topic",
        output_data_location="my-output-location",
        input_topic="projects/test-gcp-project/topics/another-topic",
        input_data_location="my-input-data",
        use_fnapi=True,
        experiments=["beam_fn_api"],
        region="us-central1",
        num_workers=2,
        max_num_workers=2,
        autoscaling_algorithm="NONE",
        disk_size_gb=32,
        worker_machine_type="A-machine",
    )
    if user_prompt_worker_image:
        expected_create_job_args.worker_image = user_prompt_worker_image

    prompt_responses = [
        expected_create_job_args.region,
        str(expected_create_job_args.use_fnapi),
        str(expected_create_job_args.create_resources),
        ",".join(expected_create_job_args.experiments),
        str(expected_create_job_args.num_workers),
        str(expected_create_job_args.max_num_workers),
        expected_create_job_args.autoscaling_algorithm,
        str(expected_create_job_args.disk_size_gb),
        expected_create_job_args.worker_machine_type,
        user_prompt_worker_image,
    ]

    if not user_prompt_worker_image:
        prompt_responses.append(expected_create_job_args.python_version)

    prompt_responses += [
        expected_create_job_args.staging_location,
        expected_create_job_args.temp_location,
        expected_create_job_args.input_topic,
        expected_create_job_args.output_topic,
        expected_create_job_args.input_data_location,
        expected_create_job_args.output_data_location,
    ]
    mock_prompt.side_effect = prompt_responses
    command_line_args = {
        "job_name": expected_create_job_args.job_name,
        "gcp_project": expected_create_job_args.gcp_project,
    }
    create_job_args = job._create_args_from_user_prompt(command_line_args)
    assert len(prompt_responses) == mock_prompt.call_count
    assert expected_create_job_args == create_job_args

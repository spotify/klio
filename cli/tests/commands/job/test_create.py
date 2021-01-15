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

import datetime
import os

import click
import mock
import pytest
import yaml

from klio_cli.commands.job import create


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "utils", "fixtures")


@pytest.fixture
def job():
    return create.CreateJob()


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def now(cls):
        return datetime.datetime(2019, 1, 1)


def test_get_environment(job):
    env = job._get_environment()
    expected_templates = [
        "MANIFEST.in.tpl",
        "README.md.tpl",
        "dockerfile.tpl",
        "init.py.tpl",
        "job-requirements.txt.tpl",
        "klio-job-batch.yaml.tpl",
        "klio-job.yaml.tpl",
        "run.py.tpl",
        "setup.py.tpl",
        "test_transforms.py.tpl",
        "transforms-batch.py.tpl",
        "transforms.py.tpl",
    ]
    assert expected_templates == sorted(env.list_templates())


def test_create_job_directory(tmpdir, job):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")
    job._create_job_directory(output_dir)

    assert os.path.exists(output_dir)


@pytest.mark.parametrize("error_code", (1, 17))
def test_create_job_directory_raises(monkeypatch, tmpdir, error_code, job):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")

    def mock_mkdir(*args, **kwargs):
        raise OSError(error_code, "some error message", output_dir)

    monkeypatch.setattr(create.os, "mkdir", mock_mkdir)

    if error_code != 17:
        with pytest.raises(OSError) as e:
            job._create_job_directory(output_dir)

        assert e.match("some error message")
    else:
        # should not re-raise if file already existsgit
        job._create_job_directory(output_dir)


def test_write_template(tmpdir, job):
    data = u"this is some test data"
    output_dir = tmpdir.mkdir("testing")
    output_file = "foo.txt"

    job._write_template(output_dir.strpath, output_file, data)

    ret_file = output_dir.join(output_file)
    ret_contents = ret_file.read()
    assert data == ret_contents


@pytest.fixture
def batch_job_context():
    return {
        "inputs": [
            {
                "event_location": "test-job_input_elements.txt",
                "data_location": "test-job-input",
            }
        ],
        "outputs": [
            {
                "event_location": "test-job_output_elements",
                "data_location": "test-job-output",
            }
        ],
    }


@pytest.fixture
def context():
    base_gcs = "gs://test-gcp-project-dataflow-tmp/test-job"
    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    return {
        "job_name": "test-job",
        "job_type": "streaming",
        "python_version": "3.6",
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
            "dependencies": [
                {
                    "job_name": "test-parent-job",
                    "gcp_project": "test-parent-gcp-project",
                    "input_topics": [
                        (
                            "projects/test-grandparent-gcp-project/topics/"
                            "test-grandparent-job-output"
                        )
                    ],
                    "region": "us-central1",
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
        "job_type": "streaming",
        "python_version": "3.6",
        "use_fnapi": False,
        "create_resources": False,
        "pipeline_options": {
            "project": "test-gcp-project",
            "region": "europe-west1",
            "worker_harness_container_image": gcr_url,
            "experiments": [],
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
            "dependencies": [],
        },
    }


@pytest.mark.parametrize("job_type", ("batch", "streaming"))
@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_job_config(
    use_fnapi, context, batch_job_context, tmpdir, monkeypatch, job, job_type
):
    output_dir = (
        tmpdir.mkdir("testing")
        .mkdir("jobs")
        .mkdir("test-job-{}".format(job_type))
    )

    env = job._get_environment()

    if job_type == "batch":
        context["job_options"] = batch_job_context

    context["use_fnapi"] = use_fnapi
    context["job_type"] = job_type
    if not use_fnapi:
        monkeypatch.setitem(context["pipeline_options"], "experiments", [])
    job._create_job_config(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    if job_type == "batch":
        fixture = os.path.join(expected_fixtures, "klio-job-batch.yaml")
    else:
        fixture = os.path.join(expected_fixtures, "klio-job.yaml")

    with open(fixture, "r") as f:
        expected = yaml.safe_load(f)
    ret_file = output_dir.join("klio-job.yaml")
    ret_contents = yaml.safe_load(ret_file.read())

    assert expected == ret_contents


@pytest.mark.parametrize("job_type", ("batch", "streaming"))
def test_create_python_files(tmpdir, mocker, job, job_type):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = job._get_environment()

    dt_patch = "klio_cli.commands.job.create.datetime.datetime"
    with mock.patch(dt_patch, MockDatetime):
        job._create_python_files(env, "test_job", job_type, output_dir.strpath)

    ret_init_file = output_dir.join("__init__.py")
    ret_init_contents = ret_init_file.read()
    ret_run_file = output_dir.join("run.py")
    ret_run_contents = ret_run_file.read()
    ret_transforms_file = output_dir.join("transforms.py")
    ret_transforms_contents = ret_transforms_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    init_fixture = os.path.join(expected_fixtures, "__init__.py")
    run_fixture = os.path.join(expected_fixtures, "run.py")
    if job_type == "batch":
        transforms_fixture = os.path.join(
            expected_fixtures, "transforms-batch.py"
        )
    else:
        transforms_fixture = os.path.join(expected_fixtures, "transforms.py")
    with open(init_fixture, "r") as f:
        expected_init = f.read()

    with open(run_fixture, "r") as f:
        expected_run = f.read()

    with open(transforms_fixture, "r") as f:
        expected_transforms = f.read()

    assert expected_init == ret_init_contents + "\n"
    assert expected_run == ret_run_contents + "\n"
    assert expected_transforms == ret_transforms_contents + "\n"


def test_create_no_fnapi_files(tmpdir, job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = job._get_environment()
    context = {"use_fnapi": False, "package_name": "test-job"}

    job._create_no_fnapi_files(env, context, output_dir.strpath)

    manifest_file = output_dir.join("MANIFEST.in")
    ret_manifest_contents = manifest_file.read()
    setup_file = output_dir.join("setup.py")
    ret_setup_contents = setup_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected", "no_fnapi")

    manifest_fixture = os.path.join(expected_fixtures, "MANIFEST.in")
    setup_fixture = os.path.join(expected_fixtures, "setup.py")
    with open(manifest_fixture, "r") as f:
        expected_manifest = f.read()

    with open(setup_fixture, "r") as f:
        expected_setup = f.read()

    assert expected_manifest == ret_manifest_contents + "\n"
    assert expected_setup == ret_setup_contents + "\n"


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_reqs_file(use_fnapi, tmpdir, job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = job._get_environment()
    context = {"use_fnapi": use_fnapi}

    job._create_reqs_file(env, context, output_dir.strpath)

    ret_reqs_file = output_dir.join("job-requirements.txt")
    ret_reqs_contents = ret_reqs_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")

    fixture = os.path.join(expected_fixtures, "job-requirements.txt")
    with open(fixture, "r") as f:
        expected = f.read()

    assert expected == ret_reqs_contents + "\n"


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_dockerfile(use_fnapi, tmpdir, job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = job._get_environment()
    context = {
        "pipeline_options": {
            "worker_harness_container_image": "gcr.io/foo/bar",
            "project": "test-gcp-project",
        },
        "python_version": "3.6",
        "use_fnapi": use_fnapi,
        "create_resources": False,
    }

    job._create_dockerfile(env, context, output_dir.strpath)

    ret_dockerfile_file = output_dir.join("Dockerfile")
    ret_dockerfile_contents = ret_dockerfile_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    fixture = os.path.join(expected_fixtures, "Dockerfile")
    with open(fixture, "r") as f:
        expected = f.read()

    assert expected == ret_dockerfile_contents


def test_create_readme(tmpdir, job):
    output_dir = tmpdir.mkdir("testing").mkdir("test_job")
    env = job._get_environment()
    context = {"job_name": "test-job"}

    job._create_readme(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")

    ret_file = output_dir.join("README.md")
    ret_file_contents = ret_file.read()
    exp_file = os.path.join(expected_fixtures, "README.md")
    with open(exp_file, "r") as f:
        exp_file_contents = f.read()

    assert exp_file_contents == ret_file_contents


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
        ("3.6", "3.6"),
        ("3.6.1", "3.6"),
        ("3.7", "3.7"),
        ("3.7.1", "3.7"),
        ("3.8", "3.8"),
        ("3.8.1", "3.8"),
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
        "job_type": "streaming",
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
        "dependencies": [
            {"job_name": "parent-job", "gcp_project": "parent-gcp-project"}
        ],
        "python_version": "3.7",
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
            "dependencies": [
                {"job_name": "parent-job", "gcp_project": "parent-gcp-project"}
            ],
        },
        "python_version": "3.7",
        "use_fnapi": False,
        "create_resources": False,
        "job_type": "streaming",
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


@pytest.mark.parametrize(
    "inputs,confirmation, regions,expected_dependencies",
    (
        (
            ["my-job", "my-proj", "my-topic", "europe-west1"],
            [False],
            ["europe-west1"],
            [
                {
                    "job_name": "my-job",
                    "gcp_project": "my-proj",
                    "input_topics": ["my-topic"],
                    "region": "europe-west1",
                }
            ],
        ),  # full input
        (
            [
                # mock confirm 1
                "my-job",
                "my-proj",
                "my-topic",
                "europe-west1",
                # mock confirm 2
                "my-job2",
                "my-proj",
                "my-topic2",
                "europe-west1",
            ],
            [True, False],
            ["europe-west1", "europe-west1"],
            [
                {
                    "job_name": "my-job",
                    "gcp_project": "my-proj",
                    "input_topics": ["my-topic"],
                    "region": "europe-west1",
                },
                {
                    "job_name": "my-job2",
                    "gcp_project": "my-proj",
                    "input_topics": ["my-topic2"],
                    "region": "europe-west1",
                },
            ],
        ),  # multiple full inputs
        (
            ["my-job", "my-proj", "", ""],
            [False],
            [],
            [{"job_name": "my-job", "gcp_project": "my-proj"}],
        ),  # use default inputs for input_topics and region
        (
            ["my-job", "my-proj", "my-topic", ""],
            [False],
            [],
            [
                {
                    "job_name": "my-job",
                    "gcp_project": "my-proj",
                    "input_topics": ["my-topic"],
                }
            ],
        ),  # use defaults for region
        (
            ["my-job", "my-proj", "", "europe-west1"],
            [False],
            ["europe-west1"],
            [
                {
                    "job_name": "my-job",
                    "gcp_project": "my-proj",
                    "region": "europe-west1",
                }
            ],
        ),  # use defaults for input topics
    ),
)
def test_get_dependencies_from_user_inputs(
    mocker,
    mock_prompt,
    mock_confirm,
    inputs,
    confirmation,
    regions,
    expected_dependencies,
    job,
):
    mock_prompt.side_effect = inputs
    mock_confirm.side_effect = confirmation

    mock_validate_region = mocker.patch.object(job, "_validate_region")
    actual_dependencies = job._get_dependencies_from_user_inputs()
    assert expected_dependencies == actual_dependencies
    expected_validate_region_calls = [mocker.call(r) for r in regions]
    assert (
        expected_validate_region_calls == mock_validate_region.call_args_list
    )


@pytest.fixture
def mock_prompt(mocker):
    return mocker.patch.object(create.click, "prompt")


@pytest.fixture
def mock_confirm(mocker):
    return mocker.patch.object(create.click, "confirm")


def test_get_context_from_user_inputs(
    context, mock_prompt, mock_confirm, mocker, job,
):
    # mimicking user inputs for each prompt
    prompt_side_effect = [
        "streaming",
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
        "3.6",
        "gs://test-gcp-project-dataflow-tmp/test-job/staging",
        "gs://test-gcp-project-dataflow-tmp/test-job/temp",
        "projects/test-parent-gcp-project/topics/test-parent-job-output",
        "projects/test-gcp-project/topics/test-job-output",
        "gs://test-parent-gcp-project-output/test-parent-job",
        "gs://test-gcp-project-output/test-job",
        # <-- mock_confirm side effect 1 -->
        "test-parent-job",
        "test-parent-gcp-project",
        (
            "projects/test-grandparent-gcp-project/topics/test-grandparent-"
            "job-output"
        ),
        "us-central1",
        # <-- mock_confirm side effect 2-->
    ]
    confirm_side_effect = [True, False]
    mock_prompt.side_effect = prompt_side_effect
    mock_confirm.side_effect = confirm_side_effect

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
    assert len(confirm_side_effect) == mock_confirm.call_count

    exp_calls = [mocker.call("europe-west1"), mocker.call("us-central1")]
    assert exp_calls == mock_validate_region.call_args_list
    assert 2 == mock_validate_region.call_count

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
    mocker,
    context_overrides,
    expected_overrides,
    mock_prompt,
    mock_confirm,
    job,
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
    expected_overrides["python_version"] = "3.6"
    assert not mock_prompt.call_count
    assert not mock_confirm.call_count
    mock_validate_region.assert_called_once_with("us-central1")
    mock_validate_worker_image.assert_called_once_with("gcr.io/foo/bar")
    assert not ret_dockerfile
    assert expected_overrides == ret_context


@pytest.mark.parametrize(
    "users_provided_dependencies,confirmed_dependencies",
    (
        (True, True),
        # (True, False), # not possible
        (False, True),
        (False, False),
    ),
)
def test_get_context_from_user_inputs_dependency_settings(
    context_overrides,
    expected_overrides,
    mock_prompt,
    mock_confirm,
    mocker,
    monkeypatch,
    users_provided_dependencies,
    confirmed_dependencies,
    job,
):
    context_overrides["machine_type"] = "n4-highmem-l33t"
    saved_dependencies = context_overrides.pop("dependencies")

    mock_get_dependencies_from_user_inputs = mocker.Mock()
    if users_provided_dependencies:
        mock_get_dependencies_from_user_inputs.return_value = (
            saved_dependencies
        )
    else:
        mock_get_dependencies_from_user_inputs.return_value = None

    monkeypatch.setattr(
        job,
        "_get_dependencies_from_user_inputs",
        mock_get_dependencies_from_user_inputs,
    )

    mock_confirm.side_effect = [confirmed_dependencies]

    mock_validate_region = mocker.patch.object(job, "_validate_region")
    mock_validate_worker_image = mocker.patch.object(
        job, "_validate_worker_image"
    )
    ret_context, ret_dockerfile = job._get_context_from_user_inputs(
        context_overrides
    )

    expected_overrides["pipeline_options"].pop("project")
    expected_overrides["python_version"] = "3.6"

    assert not mock_prompt.call_count
    assert 1 == mock_confirm.call_count
    mock_validate_region.assert_called_once_with("us-central1")
    mock_validate_worker_image.assert_called_once_with("gcr.io/foo/bar")
    assert not ret_dockerfile
    if users_provided_dependencies:
        assert expected_overrides == ret_context

    else:
        expected_overrides["job_options"]["dependencies"] = []
        assert expected_overrides == ret_context

    if confirmed_dependencies:
        assert 1 == mock_get_dependencies_from_user_inputs.call_count
    else:
        assert 0 == mock_get_dependencies_from_user_inputs.call_count


def test_get_context_from_user_inputs_no_prompts_image(
    mocker,
    context_overrides,
    expected_overrides,
    mock_prompt,
    mock_confirm,
    job,
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
    assert not mock_confirm.call_count
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
    "dependencies,expected_dependencies,num_warnings",
    (
        (
            (
                "job_name=parent-job",
                "gcp_project=a-project",
                "region=a-region",
                "input_topic=d",
                "input_topics=a,b,c",
            ),
            {
                "job_name": "parent-job",
                "gcp_project": "a-project",
                "region": "a-region",
                "input_topic": "d",
                "input_topics": ["a", "b", "c"],
            },
            0,
        ),
        (
            (
                "job-name=parent-job",
                "gcp-project=a-project",
                "region=a-region",
                "input-topic=d",
                "input-topics=a,b,c",
            ),
            {
                "job_name": "parent-job",
                "gcp_project": "a-project",
                "region": "a-region",
                "input_topic": "d",
                "input_topics": ["a", "b", "c"],
            },
            0,
        ),
        (
            ("job-name=parent-job", "gcp-project=a-project", "banana=1"),
            {"job_name": "parent-job", "gcp_project": "a-project"},
            1,
        ),
    ),
)
def test_parse_dependency_args(
    dependencies, expected_dependencies, num_warnings, caplog, job,
):
    assert expected_dependencies == job._parse_dependency_args(dependencies)
    assert num_warnings == len(caplog.records)


@pytest.mark.parametrize(
    "unknown_args,expected",
    (
        (("--foo", "foobar"), {"foo": "foobar"}),
        (
            ("--foo", "foobar", "--bar", "barfoo"),
            {"foo": "foobar", "bar": "barfoo"},
        ),
        (("--foo", "bar", "baz"), {"foo": ("bar", "baz")}),
        (
            ("--dependency", "job_name=parent-job", "gcp_project=a-project"),
            {
                "dependencies": [
                    {"job_name": "parent-job", "gcp_project": "a-project"}
                ]
            },
        ),
        (
            ("--dependency", "job_name=parent-job"),
            {"dependencies": [{"job_name": "parent-job"}]},
        ),
        (("--dependency", "banana=phone"), {},),
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
        "job_type": "streaming",
    }

    mock_get_user_input = mocker.patch.object(job, "_get_user_input")
    mock_get_user_input.return_value = (context, create_dockerfile)
    mock_get_environment = mocker.patch.object(job, "_get_environment")
    mock_create_job_dir = mocker.patch.object(job, "_create_job_directory")
    mock_create_job_config = mocker.patch.object(job, "_create_job_config")
    mock_create_no_fnapi_files = mocker.patch.object(
        job, "_create_no_fnapi_files"
    )
    mock_create_python_files = mocker.patch.object(job, "_create_python_files")
    mock_create_reqs_files = mocker.patch.object(job, "_create_reqs_file")
    mock_create_dockerfile = mocker.patch.object(job, "_create_dockerfile")
    mock_create_readme = mocker.patch.object(job, "_create_readme")

    mock_create_topics = mocker.patch.object(create.gcp_setup, "create_topics")
    mock_create_buckets = mocker.patch.object(
        create.gcp_setup, "create_buckets"
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

    job_type = context.get("job_type")
    mock_create_python_files.assert_called_once_with(
        ret_env, package_name, job_type, output_dir
    )
    if use_fnapi:
        mock_create_no_fnapi_files.assert_not_called()
    else:
        mock_create_no_fnapi_files.assert_called_once_with(
            ret_env, context, output_dir
        )

    if create_resources:
        if job_type == "streaming":
            mock_create_topics.assert_called_once_with(context)
        mock_create_buckets.assert_called_once_with(context)
        mock_create_stackdriver.assert_called_once_with(context)
    else:
        mock_create_topics.assert_not_called()
        mock_create_buckets.assert_not_called()
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


def test_get_batch_user_context(job, mock_prompt):
    kwargs = {"job_name": "test-job"}

    event_input = "events-in"
    data_input = "data-in"
    event_output = "events-out"
    data_output = "data-out"
    mock_prompt.side_effect = [
        event_input,
        data_input,
        event_output,
        data_output,
    ]

    ret_context = job._get_batch_user_input_job_context(kwargs)

    expected_context = {
        "inputs": [
            {"event_location": "events-in", "data_location": "data-in"}
        ],
        "outputs": [
            {"event_location": "events-out", "data_location": "data-out"}
        ],
    }
    assert 4 == mock_prompt.call_count

    assert expected_context == ret_context


def test_get_batch_user_context_no_prompt(job, mock_prompt):
    kwargs = {
        "job_name": "test-job",
        "batch_event_input": "input_ids.txt",
        "batch_event_output": "output_ids",
        "batch_data_input": "input-data",
        "batch_data_output": "output-data",
    }
    ret_context = job._get_batch_user_input_job_context(kwargs)

    mock_prompt.assert_not_called()

    expected_context = {
        "inputs": [
            {
                "event_location": kwargs.get("batch_event_input"),
                "data_location": kwargs.get("batch_data_input"),
            }
        ],
        "outputs": [
            {
                "event_location": kwargs.get("batch_event_output"),
                "data_location": kwargs.get("batch_data_output"),
            }
        ],
    }
    assert expected_context == ret_context


def test_get_default_batch_job_context(job):
    kwargs = {"job_name": "test-job"}

    ret_context = job._get_default_batch_job_context(kwargs)

    expected_context = {
        "inputs": [
            {
                "event_location": "test-job_input_elements.txt",
                "data_location": "test-job-input",
            }
        ],
        "outputs": [
            {
                "event_location": "test-job_output_elements",
                "data_location": "test-job-output",
            }
        ],
    }

    assert expected_context == ret_context

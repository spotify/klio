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

import mock
import pytest
import yaml

from klio_cli.commands.job import create


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "utils", "fixtures")


@pytest.fixture
def base_job():
    return create.CreateJob()


@pytest.fixture
def job():
    return create.CreateStreamingJob()


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def now(cls):
        return datetime.datetime(2019, 1, 1)


def test_get_environment(base_job):
    env = base_job._get_environment()
    expected_templates = [
        "MANIFEST.in.tpl",
        "README.md.tpl",
        "dockerfile.tpl",
        "init.py.tpl",
        "job-requirements.txt.tpl",
        "klio-job.yaml.tpl",
        "run.py.tpl",
        "setup.py.tpl",
        "test_transforms.py.tpl",
        "transforms.py.tpl",
    ]
    assert expected_templates == sorted(env.list_templates())


def test_create_job_directory(tmpdir, base_job):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")
    base_job._create_job_directory(output_dir)

    assert os.path.exists(output_dir)


@pytest.mark.parametrize("error_code", (1, 17))
def test_create_job_directory_raises(
    monkeypatch, tmpdir, error_code, base_job
):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")

    def mock_mkdir(*args, **kwargs):
        raise OSError(error_code, "some error message", output_dir)

    monkeypatch.setattr(create.os, "mkdir", mock_mkdir)

    if error_code != 17:
        with pytest.raises(OSError) as e:
            base_job._create_job_directory(output_dir)

        assert e.match("some error message")
    else:
        # should not re-raise if file already existsgit
        base_job._create_job_directory(output_dir)


def test_write_template(tmpdir, base_job):
    data = u"this is some test data"
    output_dir = tmpdir.mkdir("testing")
    output_file = "foo.txt"

    base_job._write_template(output_dir.strpath, output_file, data)

    ret_file = output_dir.join(output_file)
    ret_contents = ret_file.read()
    assert data == ret_contents


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
        "use_fnapi": False,
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


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_job_config(use_fnapi, context, tmpdir, monkeypatch, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = base_job._get_environment()

    context["use_fnapi"] = use_fnapi
    if not use_fnapi:
        monkeypatch.setitem(context["pipeline_options"], "experiments", [])
    base_job._create_job_config(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    fixture = os.path.join(expected_fixtures, "klio-job.yaml")
    with open(fixture, "r") as f:
        expected = yaml.safe_load(f)

    ret_file = output_dir.join("klio-job.yaml")
    ret_contents = yaml.safe_load(ret_file.read())

    assert expected == ret_contents


def test_create_python_files(tmpdir, mocker, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = base_job._get_environment()

    dt_patch = "klio_cli.commands.job.create.datetime.datetime"
    with mock.patch(dt_patch, MockDatetime):
        base_job._create_python_files(env, output_dir.strpath)

    ret_init_file = output_dir.join("__init__.py")
    ret_init_contents = ret_init_file.read()
    ret_run_file = output_dir.join("run.py")
    ret_run_contents = ret_run_file.read()
    ret_transforms_file = output_dir.join("transforms.py")
    ret_transforms_contents = ret_transforms_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    init_fixture = os.path.join(expected_fixtures, "__init__.py")
    run_fixture = os.path.join(expected_fixtures, "run.py")
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


def test_create_no_fnapi_files(tmpdir, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = base_job._get_environment()
    context = {"job_name": "test-job", "use_fnapi": False}

    base_job._create_no_fnapi_files(env, context, output_dir.strpath)

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
def test_create_reqs_file(use_fnapi, tmpdir, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = base_job._get_environment()
    context = {"use_fnapi": use_fnapi}

    base_job._create_reqs_file(env, context, output_dir.strpath)

    ret_reqs_file = output_dir.join("job-requirements.txt")
    ret_reqs_contents = ret_reqs_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    fixture = os.path.join(expected_fixtures, "job-requirements.txt")
    with open(fixture, "r") as f:
        expected = f.read()

    assert expected == ret_reqs_contents + "\n"


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_dockerfile(use_fnapi, tmpdir, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = base_job._get_environment()
    context = {
        "pipeline_options": {
            "worker_harness_container_image": "gcr.io/foo/bar",
            "project": "test-gcp-project",
        },
        "python_version": "36",
        "use_fnapi": use_fnapi,
        "create_resources": False,
    }

    base_job._create_dockerfile(env, context, output_dir.strpath)

    ret_dockerfile_file = output_dir.join("Dockerfile")
    ret_dockerfile_contents = ret_dockerfile_file.read()

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    fixture = os.path.join(expected_fixtures, "Dockerfile")
    with open(fixture, "r") as f:
        expected = f.read()

    assert expected == ret_dockerfile_contents


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_readme(use_fnapi, tmpdir, base_job):
    output_dir = tmpdir.mkdir("testing").mkdir("test_job")
    env = base_job._get_environment()
    context = {"job_name": "test-job", "use_fnapi": use_fnapi}

    base_job._create_readme(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    ret_file = output_dir.join("README.md")
    ret_file_contents = ret_file.read()
    exp_file = os.path.join(expected_fixtures, "README.md")
    with open(exp_file, "r") as f:
        exp_file_contents = f.read()

    assert exp_file_contents == ret_file_contents


def test_parse_cli_args(base_job):
    known_args = {"job_name": "job"}
    addl_job_args = {"worker_machine_type": "lazy"}

    base_job._parse_cli_args(known_args, addl_job_args)

    for k in base_job.create_args_dict:
        if k not in ("job_name", "worker_machine_type"):
            assert base_job.create_args_dict.get(k) is None

    assert known_args["job_name"] == base_job.create_args_dict.get("job_name")
    assert addl_job_args[
        "worker_machine_type"
    ] == base_job.create_args_dict.get("worker_machine_type")


def test_get_context_from_defaults(default_context, job):
    job.build_create_job_args(
        {"job_name": "test-job", "use_defaults": True},
        ("--gcp-project", "test-gcp-project"),
    )
    ret_context = job._get_context()

    assert default_context == ret_context


@pytest.fixture
def addl_job_args_overrides():
    return {
        "gcp_project": "test-gcp-project",
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
        "worker_image": "gcr.io/foo/bar",
    }


def convert_to_args(override_dict):
    """
    Converts override_dict to tuple of command line args

    Example:
        {"foo": "bar"} -> ("--foo", "bar")
    """
    new_keys = ["--{}".format(k) for k in override_dict.keys()]
    new_values = [str(v) for v in override_dict.values()]
    arg_pairs = zip(new_keys, new_values)
    args = [item for kv in arg_pairs for item in kv]
    return args


@pytest.fixture
def known_args_overrides():
    return {
        "job_name": "test-job",
        "python_version": "37",
        "use_fnapi": "n",
        "create_resources": "n",
    }


@pytest.fixture
def expected_overrides():
    return {
        "job_name": "test-job",
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
    known_args_overrides, addl_job_args_overrides, expected_overrides, job
):
    # FYI: Click will pass in kwargs as a flat dict
    addl_job_args_overrides["worker_machine_type"] = "n4-highmem-l33t"
    addl_job_args = convert_to_args(addl_job_args_overrides)
    job.build_create_job_args(known_args_overrides, addl_job_args)
    ret_context = job._get_context()

    assert expected_overrides == ret_context


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
        "Y",
        "n",
        "",
        "36",
        ["beam_fn_api"],
        "europe-west1",
        2,
        2,
        "NONE",
        32,
        "n1-standard-2",
        "gs://test-gcp-project-dataflow-tmp/test-job/staging",
        "gs://test-gcp-project-dataflow-tmp/test-job/temp",
        "projects/test-parent-gcp-project/topics/test-parent-job-output",
        "projects/test-gcp-project/topics/test-job-output",
        "gs://test-parent-gcp-project-output/test-parent-job",
        "gs://test-gcp-project-output/test-job",
    ]
    confirm_side_effect = []
    mock_prompt.side_effect = prompt_side_effect
    mock_confirm.side_effect = confirm_side_effect

    job.build_create_job_args(
        {"job_name": "test-job", "use_defaults": False},
        ("--gcp_project", "test-gcp-project"),
    )
    ret_context = job._get_context()

    assert len(prompt_side_effect) == mock_prompt.call_count
    assert len(confirm_side_effect) == mock_confirm.call_count

    context["use_fnapi"] = True
    context["create_resources"] = False
    assert context == ret_context
    assert job.create_args_dict.get("create_dockerfile")


def test_get_context_from_user_inputs_no_prompts(
    mocker,
    known_args_overrides,
    addl_job_args_overrides,
    expected_overrides,
    mock_prompt,
    mock_confirm,
    job,
):
    addl_job_args_overrides["worker_machine_type"] = "n4-highmem-l33t"
    addl_job_args = convert_to_args(addl_job_args_overrides)
    job.build_create_job_args(known_args_overrides, addl_job_args)
    ret_context = job._get_context()

    expected_overrides["python_version"] = "37"
    assert not mock_prompt.call_count
    assert not mock_confirm.call_count
    assert not job.create_args_dict.get("create_dockerfile")
    assert expected_overrides == ret_context


def test_get_context_from_user_inputs_no_prompts_image(
    mocker,
    known_args_overrides,
    addl_job_args_overrides,
    expected_overrides,
    mock_prompt,
    mock_confirm,
    job,
):
    mock_prompt.side_effect = [""]

    addl_job_args_overrides.pop("worker_image")
    addl_job_args_overrides["worker_machine_type"] = "n4-highmem-l33t"
    addl_job_args = convert_to_args(addl_job_args_overrides)
    job.build_create_job_args(known_args_overrides, addl_job_args)
    ret_context = job._get_context()

    gcr_url = "gcr.io/test-gcp-project/test-job-worker"
    exp_pipeline_opts = expected_overrides["pipeline_options"]
    exp_pipeline_opts["worker_harness_container_image"] = gcr_url

    assert 1 == mock_prompt.call_count
    assert not mock_confirm.call_count
    assert job.create_args_dict.get("create_dockerfile")
    assert expected_overrides == ret_context


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

    mock_get_context = mocker.patch.object(job, "_get_context")
    mock_get_context.return_value = context
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

    mock_create_topics = mocker.patch.object(
        create.gcp_setup, "create_topics_and_buckets"
    )
    mock_create_stackdriver = mocker.patch.object(
        create.gcp_setup, "create_stackdriver_dashboard"
    )

    unknown_args = ("--foo", "bar", "--use_fnapi", str(use_fnapi))
    known_args = {
        "job_name": "test-job",
        "gcp_project": "test-gcp-project",
        "job_dir": "/testing/dir",
        "use_defaults": True,
    }
    job.create(unknown_args, known_args)

    known_args["foo"] = "bar"
    mock_get_context.assert_called_once_with()

    mock_get_environment.assert_called_once_with()

    ret_env = mock_get_environment.return_value
    mock_create_job_dir.assert_called_once_with(known_args["job_dir"])

    mock_create_job_config.assert_called_once_with(
        ret_env, context, known_args["job_dir"]
    )

    mock_create_python_files.assert_called_once_with(
        ret_env, known_args["job_dir"]
    )
    if use_fnapi:
        mock_create_no_fnapi_files.assert_not_called()
    else:
        mock_create_no_fnapi_files.assert_called_once_with(
            ret_env, context, known_args["job_dir"]
        )

    if create_resources:
        mock_create_topics.assert_called_once_with(context)
        mock_create_stackdriver.assert_called_once_with(context)
    else:
        mock_create_topics.assert_not_called()
        mock_create_stackdriver.assert_not_called()

    mock_create_reqs_files.assert_called_once_with(
        ret_env, context, known_args["job_dir"]
    )
    if create_dockerfile:
        mock_create_dockerfile.assert_called_once_with(
            ret_env, context, known_args["job_dir"]
        )
    mock_create_readme.assert_called_once_with(
        ret_env, context, known_args["job_dir"]
    )
    assert 1 == len(caplog.records)

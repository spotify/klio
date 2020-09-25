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

from klio_cli.commands.job.utils import rendering


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "fixtures")


# pytest prevents monkeypatching datetime directly
class MockDatetime(datetime.datetime):
    @classmethod
    def now(cls):
        return datetime.datetime(2019, 1, 1)


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
def template_renderer():
    return rendering.CreateJobTemplateRenderer()


def test_get_environment(template_renderer):
    env = template_renderer.get_environment()
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


def test_write_template(template_renderer, tmpdir):
    data = u"this is some test data"
    output_dir = tmpdir.mkdir("testing")
    output_file = "foo.txt"

    template_renderer.write_template(output_dir.strpath, output_file, data)

    ret_file = output_dir.join(output_file)
    ret_contents = ret_file.read()
    assert data == ret_contents


@pytest.mark.parametrize("use_fnapi", (True, False))
def test_create_job_config(
    use_fnapi, context, tmpdir, monkeypatch, template_renderer
):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = template_renderer.get_environment()

    context["use_fnapi"] = use_fnapi
    if not use_fnapi:
        monkeypatch.setitem(context["pipeline_options"], "experiments", [])
    template_renderer.create_job_config(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    fixture = os.path.join(expected_fixtures, "klio-job.yaml")
    with open(fixture, "r") as f:
        expected = yaml.safe_load(f)

    ret_file = output_dir.join("klio-job.yaml")
    ret_contents = yaml.safe_load(ret_file.read())

    assert expected == ret_contents


def test_create_python_files(tmpdir, template_renderer):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = template_renderer.get_environment()

    dt_patch = "klio_cli.commands.job.utils.rendering.datetime.datetime"
    with mock.patch(dt_patch, MockDatetime):
        template_renderer.create_python_files(
            env, "test_job", output_dir.strpath
        )

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


def test_create_no_fnapi_files(tmpdir, template_renderer):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = template_renderer.get_environment()
    context = {"use_fnapi": False, "package_name": "test-job"}

    template_renderer.create_no_fnapi_files(env, context, output_dir.strpath)

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
def test_create_reqs_file(use_fnapi, tmpdir, template_renderer):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = template_renderer.get_environment()
    context = {"use_fnapi": use_fnapi}

    template_renderer.create_reqs_file(env, context, output_dir.strpath)

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
def test_create_dockerfile(use_fnapi, tmpdir, template_renderer):
    output_dir = tmpdir.mkdir("testing").mkdir("jobs").mkdir("test_job")
    env = template_renderer.get_environment()
    context = {
        "pipeline_options": {
            "worker_harness_container_image": "gcr.io/foo/bar",
            "project": "test-gcp-project",
        },
        "python_version": "36",
        "use_fnapi": use_fnapi,
        "create_resources": False,
    }

    template_renderer.create_dockerfile(env, context, output_dir.strpath)

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
def test_create_readme(use_fnapi, tmpdir, template_renderer):
    output_dir = tmpdir.mkdir("testing").mkdir("test_job")
    env = template_renderer.get_environment()
    context = {"job_name": "test-job", "use_fnapi": use_fnapi}

    template_renderer.create_readme(env, context, output_dir.strpath)

    expected_fixtures = os.path.join(FIXTURE_PATH, "expected")
    is_fnapi_dir = "fnapi" if use_fnapi else "no_fnapi"
    expected_fixtures = os.path.join(expected_fixtures, is_fnapi_dir)

    ret_file = output_dir.join("README.md")
    ret_file_contents = ret_file.read()
    exp_file = os.path.join(expected_fixtures, "README.md")
    with open(exp_file, "r") as f:
        exp_file_contents = f.read()

    assert exp_file_contents == ret_file_contents


def test_create_job_directory(tmpdir, template_renderer):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")
    template_renderer.create_job_directory(output_dir)

    assert os.path.exists(output_dir)


@pytest.mark.parametrize("error_code", (1, 17))
def test_create_job_directory_raises(
    monkeypatch, tmpdir, error_code, template_renderer
):
    output_base_dir = tmpdir.mkdir("testing")
    output_dir = os.path.join(str(output_base_dir), "test_job")

    def mock_mkdir(*args, **kwargs):
        raise OSError(error_code, "some error message", output_dir)

    monkeypatch.setattr(os, "mkdir", mock_mkdir)

    if error_code != 17:
        with pytest.raises(OSError) as e:
            template_renderer.create_job_directory(output_dir)

        assert e.match("some error message")
    else:
        # should not re-raise if file already exists
        template_renderer.create_job_directory(output_dir)

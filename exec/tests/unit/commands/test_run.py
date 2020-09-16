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
from unittest import mock

import pytest

from apache_beam.options import pipeline_options

from klio import __version__ as klio_lib_version
from klio_core import __version__ as klio_core_version

from klio_exec import __version__ as klio_exec_version


@pytest.fixture
def options():
    return pipeline_options.PipelineOptions()


def _gcp_options():
    return {
        "project": "test-project",
        "region": "us-central1",
        "temp_location": "gs://temp/location",
        "staging_location": "gs://staging/location",
        "dataflow_endpoint": "api.example.com",
        "service_account_email": "sudo@example.com",
        "no_auth": False,
        "template_location": "gs://template/location",
        "enable_streaming_engine": True,
        "dataflow_kms_key": "some_key",
        "flexrs_goal": "SPEED_OPTIMIZED",
        "labels": ["foo=bar", "baz=bla"],
    }


def _worker_options():
    return {
        "subnetwork": "projects/foo/regions/bar/network",
        "worker_machine_type": "n1-standard-2",
        "disk_size_gb": 32,
        "autoscaling_algorithm": "NONE",
        "num_workers": 2,
        "max_num_workers": 10,
        "worker_disk_type": (
            "compute.googleapis.com/projects/test-project/regions/"
            "us-central1/diskTypes/pd-ssd"
        ),
        "use_public_ips": False,
        "min_cpu_platform": "Intel Milkyway",
        "dataflow_worker_jar": "/foo/bar.ajr",
        "worker_harness_container_image": "gcr.io/example/image",
        "experiments": [],
        "streaming": True,
        "none_param": None,
    }


def _all_options():
    all_options = _gcp_options().copy()
    all_options.update(_worker_options())
    return all_options


@pytest.fixture
def pipeline_options_from_dict(all_options):
    return pipeline_options.PipelineOptions().from_dictionary(all_options)


def _config():
    mock_config = mock.Mock()
    mock_config.job_name = "my-job"
    mock_output = mock.Mock()
    mock_output.topic = "my-topic"
    mock_config.version = 2

    mock_input = mock.Mock()
    mock_input.subscription = "my-subscription"
    # Batch configs
    mock_input.name = "file"
    mock_input.location = "gs://sigint/dummy-file.txt"
    mock_input.to_io_kwargs.return_value = {
        "location": "gs://sigint/dummy-file.txt",
    }
    mock_output.name = "file"
    mock_output.location = "gs://sigint/"
    mock_output.to_io_kwargs.return_value = {
        "location": "gs://sigint/",
    }
    mock_job_config = mock.Mock()
    mock_job_config.events.inputs = [mock_input]
    mock_job_config.events.outputs = [mock_output]
    mock_job_config.data.inputs = [mock_input]
    mock_job_config.data.outputs = [mock_output]

    mock_pipeline_options = mock.Mock()

    for option, value in _gcp_options().items():
        setattr(mock_pipeline_options, option, value)

    for option, value in _worker_options().items():
        setattr(mock_pipeline_options, option, value)

    mock_pipeline_options.experiments = []
    mock_pipeline_options.streaming = True

    mock_pipeline_options.as_dict.return_value = _all_options()

    mock_config.job_config = mock_job_config
    mock_config.pipeline_options = mock_pipeline_options

    return mock_config


from klio_exec.commands import run  # noqa E402


@pytest.fixture
def gcp_options():
    return _gcp_options()


@pytest.fixture
def worker_options():
    return _worker_options()


@pytest.fixture
def all_options():
    return _all_options()


@pytest.fixture
def config():
    return _config()


@pytest.mark.parametrize("run_callable", ["run", "run_basic"])
def test_get_run_callable(monkeypatch, mocker, run_callable):
    mock_run_module = mocker.Mock()
    expected_run_callable = mocker.Mock()

    if run_callable == "run":
        mock_run_module.return_value.run = expected_run_callable
        mock_run_module.return_value.run_basic = None
    else:
        mock_run_module.return_value.run = None
        mock_run_module.return_value.run_basic = expected_run_callable

    monkeypatch.setattr(run.imp, "load_source", mock_run_module)

    kpipe = run.KlioPipeline("my-job", mocker.Mock(), mocker.Mock())
    actual_callable = kpipe._get_run_callable()

    assert expected_run_callable == actual_callable


@pytest.mark.parametrize("exception", [None, ImportError, IOError])
def test_get_run_callable_raises(mocker, monkeypatch, caplog, exception):
    mock_run_module = mocker.Mock()

    if not exception:
        mock_run_module.return_value.run = None
        mock_run_module.return_value.run_basic = None
    else:
        mock_run_module.side_effect = exception

    monkeypatch.setattr(run.imp, "load_source", mock_run_module)

    kpipe = run.KlioPipeline("my-job", mocker.Mock(), mocker.Mock())

    with pytest.raises(SystemExit):
        kpipe._get_run_callable()

    assert 1 == len(caplog.records)


@pytest.mark.parametrize(
    "image,tag,expected_image",
    (
        ("gcr.io/foo/image", None, "gcr.io/foo/image"),
        ("gcr.io/foo/image:latest", None, "gcr.io/foo/image:latest"),
        ("gcr.io/foo/image", "my-tag", "gcr.io/foo/image:my-tag"),
        ("gcr.io/foo/image:latest", "my-tag", "gcr.io/foo/image:my-tag"),
    ),
)
def test_get_image_tag(image, tag, expected_image):
    actual_image = run.KlioPipeline._get_image_tag(image, tag)
    assert expected_image == actual_image


@pytest.mark.parametrize(
    "exp,setup_file,requirements_file",
    [
        ("beam_fn_api", None, None),
        (None, "setup.py", None),
        (None, "setup.py", "requirements.txt"),
        (None, None, "requirements.txt"),
    ],
)
def test_verify_packaging(exp, setup_file, requirements_file, mocker):
    mock_config = mocker.Mock()
    mock_config.pipeline_options = mocker.Mock()
    mock_config.pipeline_options.experiments = [exp]
    mock_config.pipeline_options.setup_file = setup_file
    mock_config.pipeline_options.requirements_file = requirements_file

    kpipe = run.KlioPipeline("test-job", mock_config, mocker.Mock())

    kpipe._verify_packaging()


def test_verify_packaging_raises(mocker):
    mock_config = mocker.Mock()
    mock_config.pipeline_options = mocker.Mock()
    mock_config.pipeline_options.experiments = ["beam_fn_api"]
    mock_config.pipeline_options.setup_file = "setup.py"

    kpipe = run.KlioPipeline("test-job", mock_config, mocker.Mock())

    with pytest.raises(SystemExit):
        kpipe._verify_packaging()


@pytest.mark.parametrize(
    "setup_file,reqs_file",
    (
        (None, None),
        ("setup.py", None),
        (None, "job-requirements.txt"),
        ("setup.py", "job-requirements.txt"),
    ),
)
def test_set_setup_options(
    all_options, config, setup_file, reqs_file, mocker, monkeypatch
):
    here = os.path.abspath(".")

    if reqs_file:
        all_options["requirements_file"] = reqs_file
    if setup_file:
        all_options["setup_file"] = setup_file

    options = pipeline_options.PipelineOptions().from_dictionary(all_options)
    actual_setup_options = options.view_as(pipeline_options.SetupOptions)

    kpipe = run.KlioPipeline("test-job", config, mocker.Mock())

    kpipe._set_setup_options(options)

    if setup_file:
        expected_setup_file = os.path.join(here, setup_file)
        assert expected_setup_file == actual_setup_options.setup_file
    else:
        assert actual_setup_options.setup_file is None
    if reqs_file:
        expected_reqs_file = os.path.join(here, reqs_file)
        assert expected_reqs_file == actual_setup_options.requirements_file
    else:
        assert actual_setup_options.requirements_file is None


def test_set_debug_options(config, mocker):
    test_options = mocker.Mock()
    kpipe = run.KlioPipeline("test-job", config, mocker.Mock())
    kpipe._set_debug_options(test_options)


@pytest.mark.parametrize("direct_runner", (True, False))
def test_set_standard_options(
    all_options, config, direct_runner, mocker, monkeypatch
):
    all_options["runner"] = "dataflow"
    options = pipeline_options.PipelineOptions().from_dictionary(all_options)
    actual_std_options = options.view_as(pipeline_options.StandardOptions)

    runtime_conf = mocker.Mock(direct_runner=direct_runner)
    kpipe = run.KlioPipeline("test-job", config, runtime_conf)
    kpipe._set_standard_options(options)

    assert actual_std_options.streaming is True

    expected_runner = "direct" if direct_runner else "dataflow"
    assert expected_runner == actual_std_options.runner


@pytest.mark.parametrize("fn_api_enabled", [True, False])
def test_set_worker_options(
    pipeline_options_from_dict,
    worker_options,
    config,
    fn_api_enabled,
    mocker,
    monkeypatch,
):
    expected_opts = [
        "subnetwork",
        "disk_size_gb",
        "autoscaling_algorithm",
        "num_workers",
        "max_num_workers",
        "use_public_ips",
        "min_cpu_platform",
        "dataflow_worker_jar",
    ]

    if fn_api_enabled:
        monkeypatch.setattr(
            config.pipeline_options, "experiments", ["beam_fn_api"]
        )

    kpipe = run.KlioPipeline("test-job", config, mocker.Mock(image_tag="foo"))
    kpipe._set_worker_options(pipeline_options_from_dict)

    actual_worker_options = pipeline_options_from_dict.view_as(
        pipeline_options.WorkerOptions
    )
    for opt in expected_opts:
        expected_value = worker_options[opt]
        # The True/False values in worker opts represent flags in the options.
        # These values are set to None when you set the PipelineOptions from
        # a dictionary. Since beam uses argparse to set values, None represents
        # False for these flags.
        # https://docs.python.org/2/howto/argparse.html
        # https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py#L723
        if not expected_value:
            expected_value = None
        # getattr should explode when not setting a default value
        assert expected_value == getattr(actual_worker_options, opt)

    assert (
        worker_options["worker_machine_type"]
        == actual_worker_options.machine_type
    )
    assert (
        worker_options["worker_disk_type"] == actual_worker_options.disk_type
    )

    if fn_api_enabled:
        assert (
            "gcr.io/example/image:foo"
            == actual_worker_options.worker_harness_container_image
        )
    else:
        assert (
            "gcr.io/example/image"
            == actual_worker_options.worker_harness_container_image
        )


@pytest.mark.parametrize(
    "update,exp_update,dataflow_endpoint,klio_cli_version,deployed_ci,user_env",
    [
        (True, True, None, None, False, True),
        (False, False, None, "1.2.3", True, True),
        (None, False, "api.example.com", None, False, False),
    ],
)
def test_set_google_cloud_options(
    all_options,
    config,
    update,
    exp_update,
    dataflow_endpoint,
    klio_cli_version,
    deployed_ci,
    user_env,
    gcp_options,
    mocker,
    monkeypatch,
):
    expected_opts = [
        "project",
        "region",
        "temp_location",
        "staging_location",
        "service_account_email",
        "no_auth",
        "template_location",
        "enable_streaming_engine",
        "dataflow_kms_key",
        "flexrs_goal",
    ]
    # this is to be changed when running `tox`; remove when no longer
    # supporting beam 2.14.0

    if dataflow_endpoint:
        all_options["dataflow_endpoint"] = dataflow_endpoint
    else:
        all_options.pop("dataflow_endpoint", None)

    options = pipeline_options.PipelineOptions().from_dictionary(all_options)

    actual_gcp_opts = options.view_as(pipeline_options.GoogleCloudOptions)

    monkeypatch.setattr(
        config.pipeline_options, "dataflow_endpoint", dataflow_endpoint
    )
    if klio_cli_version:
        monkeypatch.setenv("KLIO_CLI_VERSION", klio_cli_version)
        klio_cli_version_clean = klio_cli_version.replace(".", "-")
    if deployed_ci:
        monkeypatch.setenv("CI", "TRUE")
    if not user_env:
        monkeypatch.delenv("USER", raising=False)

    kpipe = run.KlioPipeline("test-job", config, mocker.Mock(update=update))
    kpipe._set_google_cloud_options(options)

    for opt in expected_opts:
        expected_value = gcp_options[opt]
        # getattr should explode when not setting a default value
        assert expected_value == getattr(actual_gcp_opts, opt)

    assert exp_update == actual_gcp_opts.update
    if dataflow_endpoint:
        assert dataflow_endpoint == actual_gcp_opts.dataflow_endpoint
    else:
        assert (
            "https://dataflow.googleapis.com"
            == actual_gcp_opts.dataflow_endpoint
        )
    user = None
    if deployed_ci:
        user = "CI"
    elif user_env:
        user = os.environ["USER"]

    klio_exec_version_clean = klio_exec_version.replace(".", "-")
    klio_core_version_clean = klio_core_version.replace(".", "-")
    klio_lib_version_clean = klio_lib_version.replace(".", "-")
    exp_labels = [
        "foo=bar",
        "baz=bla",
        "klio-exec={}".format(klio_exec_version_clean),
        "klio-core={}".format(klio_core_version_clean),
        "klio={}".format(klio_lib_version_clean),
    ]
    if user:
        exp_labels.append("deployed_by={}".format(user).lower())
    if klio_cli_version:
        exp_labels.append("klio-cli={}".format(klio_cli_version_clean))
    assert sorted(exp_labels) == sorted(actual_gcp_opts.labels)


def test_pipeline_options_from_dictionary(options):
    test_dict = {
        "random_param": "wombat",
        "project": "test_project",
        "subnetwork": "test_subnetwork",
        "num_workers": 2,
        "worker_machine_type": "test_machine_type",
        "use_public_ips": False,
    }

    test_options = options.from_dictionary(test_dict)
    google_options = test_options.view_as(pipeline_options.GoogleCloudOptions)
    worker_options = test_options.view_as(pipeline_options.WorkerOptions)

    assert "test_project" == google_options.project
    assert "test_subnetwork" == worker_options.subnetwork
    assert 2 == worker_options.num_workers
    assert "test_machine_type" == worker_options.machine_type
    assert worker_options.use_public_ips is None

    # This is testing that the options are created with the random param,
    # but the param is not accessible since it would not be recognized by
    # DataFlow.
    with pytest.raises(KeyError):
        test_options.get_all_options()["random_param"]


@pytest.mark.parametrize(
    "setup_file,reqs_file",
    ((None, None), ("setup.py", None), (None, "reqs.txt")),
)
def test_parse_config_pipeline_options(
    setup_file, reqs_file, all_options, config, mocker, monkeypatch
):
    as_dict_ret = config.pipeline_options.as_dict.return_value
    if setup_file:
        monkeypatch.setitem(as_dict_ret, "setup_file", setup_file)
        all_options["setup_file"] = setup_file
        all_options.pop("worker_harness_container_image")
    if reqs_file:
        monkeypatch.setitem(as_dict_ret, "requirements_file", reqs_file)
        all_options["requirements_file"] = reqs_file
        all_options.pop("worker_harness_container_image")
    kpipe = run.KlioPipeline("test-job", config, mocker.Mock())
    actual = kpipe._parse_config_pipeline_options()

    all_options.pop("none_param")

    assert all_options == actual


@pytest.mark.parametrize("has_none_values", [True, False])
def test_get_pipeline_options(
    has_none_values,
    config,
    options,
    worker_options,
    gcp_options,
    mocker,
    monkeypatch,
):

    mock_parse_config_pipeline_opts = mocker.Mock()
    mock_set_gcp_opts = mocker.Mock()
    mock_set_worker_opts = mocker.Mock()
    mock_set_std_opts = mocker.Mock()
    mock_set_debug_opts = mocker.Mock()
    mock_set_setup_opts = mocker.Mock()
    mock_opts = mocker.Mock()
    mock_opts_from_dict = mocker.Mock()

    monkeypatch.setattr(
        run.KlioPipeline,
        "_parse_config_pipeline_options",
        mock_parse_config_pipeline_opts,
    )

    monkeypatch.setattr(
        run.KlioPipeline, "_set_google_cloud_options", mock_set_gcp_opts
    )

    monkeypatch.setattr(
        run.KlioPipeline, "_set_worker_options", mock_set_worker_opts
    )

    monkeypatch.setattr(
        run.KlioPipeline, "_set_standard_options", mock_set_std_opts
    )

    monkeypatch.setattr(
        run.KlioPipeline, "_set_debug_options", mock_set_debug_opts
    )

    monkeypatch.setattr(
        run.KlioPipeline, "_set_setup_options", mock_set_setup_opts
    )

    monkeypatch.setattr(
        run.pipeline_options, "PipelineOptions", lambda: mock_opts
    )

    mock_opts_from_dict = mock_opts.from_dictionary.return_value

    mock_runtime_conf = mocker.Mock()

    kpipe = run.KlioPipeline("test-job", config, mock_runtime_conf)

    kpipe._get_pipeline_options()

    mock_parse_config_pipeline_opts.assert_called_once_with()

    mock_set_gcp_opts.assert_called_once_with(mock_opts_from_dict)
    mock_set_worker_opts.assert_called_once_with(mock_opts_from_dict)
    mock_set_std_opts.assert_called_once_with(mock_opts_from_dict)
    mock_set_debug_opts.assert_called_once_with(mock_opts_from_dict)
    mock_set_setup_opts.assert_called_once_with(mock_opts_from_dict)


@pytest.mark.parametrize(
    "direct_runner,run_error,exp_call_count",
    (
        (True, None, 1),
        (False, None, 1),
        (True, ValueError("No running job found with name"), 2),
    ),
)
@pytest.mark.parametrize("blocking", (True, False))
@pytest.mark.parametrize("streaming", (True, False))
def test_run_pipeline(
    streaming,
    blocking,
    direct_runner,
    run_error,
    exp_call_count,
    config,
    mocker,
    monkeypatch,
):
    job_name = "my-job"
    mock_runtime_config = mocker.Mock(
        direct_runner=direct_runner, update=True, blocking=blocking
    )

    mock_verify_packaging = mocker.Mock()
    mock_get_run_callable = mocker.Mock()
    mock_run_callable = mocker.Mock()
    mock_run_callable.return_value.__or__ = mocker.Mock()
    mock_get_run_callable.return_value = mock_run_callable
    mock_get_pipeline_options = mocker.Mock()
    mock_pipeline = mocker.Mock()
    mock_pipeline.return_value.__or__ = mocker.Mock()
    mock_read_from_pubsub = mocker.Mock()
    mock_read_from_pubsub.return_value.__or__ = mocker.Mock()
    mock_read_from_file = mocker.Mock()
    mock_write_to_file = mocker.Mock()
    mock_write_to_pubsub = mocker.Mock()
    mock_write_to_pubsub.return_value.__or__ = mocker.Mock()
    monkeypatch.setattr(
        run.BatchEventMapper, "input", {"file": mock_read_from_file},
    )
    monkeypatch.setattr(
        run.BatchEventMapper, "output", {"file": mock_write_to_file},
    )
    monkeypatch.setattr(
        run.StreamingEventMapper, "input", {"pubsub": mock_read_from_pubsub},
    )
    monkeypatch.setattr(
        run.StreamingEventMapper, "output", {"pubsub": mock_write_to_pubsub},
    )
    monkeypatch.setattr(
        run.KlioPipeline, "_verify_packaging", mock_verify_packaging
    )
    monkeypatch.setattr(
        run.KlioPipeline, "_get_run_callable", mock_get_run_callable
    )
    monkeypatch.setattr(
        run.KlioPipeline, "_get_pipeline_options", mock_get_pipeline_options
    )
    monkeypatch.setattr(run.beam, "Pipeline", mock_pipeline)
    monkeypatch.setattr(run.beam.io, "ReadFromPubSub", mock_read_from_pubsub)
    monkeypatch.setattr(run.beam.io, "WriteToPubSub", mock_write_to_pubsub)
    if streaming:
        mock_input = mocker.Mock()
        mock_input.name = "pubsub"
        mock_input.to_io_kwargs.return_value = {
            "subscription": "projects/foo/subscriptions/bar",
        }
        mock_output = mocker.Mock()
        mock_output.name = "pubsub"
        mock_output.to_io_kwargs.return_value = {
            "topic": "projects/foo/topics/bar",
        }
        config.job_config.events.inputs = [mock_input]
        config.job_config.events.outputs = [mock_output]

    if run_error:
        mock_pipeline.return_value.run.side_effect = [
            run_error,
            mock_pipeline.return_value.run.return_value,
        ]

    config.pipeline_options.streaming = streaming

    kpipe = run.KlioPipeline(job_name, config, mock_runtime_config)

    kpipe.run()

    assert exp_call_count == mock_verify_packaging.call_count
    mock_verify_packaging.assert_called_with()

    assert exp_call_count == mock_get_run_callable.call_count
    mock_get_run_callable.assert_called_with()

    assert exp_call_count == mock_get_pipeline_options.call_count
    mock_get_pipeline_options.assert_called_with()

    assert (
        exp_call_count
        == mock_get_pipeline_options.return_value.view_as.call_count
    )
    mock_get_pipeline_options.return_value.view_as.assert_called_with(
        pipeline_options.SetupOptions
    )

    assert exp_call_count == mock_pipeline.call_count
    mock_pipeline.assert_called_with(
        options=mock_get_pipeline_options.return_value
    )

    assert exp_call_count == mock_pipeline.return_value.run.call_count

    if direct_runner or blocking:
        result = mock_pipeline.return_value.run.return_value
        result.wait_until_finish.assert_called_once_with()


@pytest.mark.parametrize(
    "update,value_err_msg",
    (
        (False, "No running job found with name"),
        (True, "A different error"),
        (False, "A different error"),
    ),
)
def test_run_pipeline_raises(
    update, value_err_msg, config, mocker, monkeypatch, caplog
):
    job_name = "my-job"
    mock_runtime_config = mocker.Mock(update=update)

    mock_verify_packaging = mocker.Mock()
    mock_get_run_callable = mocker.Mock()
    mock_run_callable = mocker.Mock()
    mock_run_callable.return_value.__or__ = mocker.Mock()
    mock_get_run_callable.return_value = mock_run_callable
    mock_get_pipeline_options = mocker.Mock()
    mock_pipeline = mocker.Mock()
    mock_pipeline.return_value.__or__ = mocker.Mock()
    mock_read_from_pubsub = mocker.Mock()
    mock_read_from_pubsub.return_value.__or__ = mocker.Mock()
    mock_read_from_file = mocker.Mock()
    mock_read_from_file.return_value.__or__ = mocker.Mock()
    mock_write_to_file = mocker.Mock()
    mock_write_to_file.return_value.__or__ = mocker.Mock()
    mock_write_to_pubsub = mocker.Mock()
    mock_write_to_pubsub.return_value.__or__ = mocker.Mock()
    monkeypatch.setattr(
        run.KlioPipeline, "_verify_packaging", mock_verify_packaging
    )
    monkeypatch.setattr(
        run.KlioPipeline, "_get_run_callable", mock_get_run_callable
    )
    monkeypatch.setattr(
        run.KlioPipeline, "_get_pipeline_options", mock_get_pipeline_options
    )
    monkeypatch.setattr(run.beam, "Pipeline", mock_pipeline)
    monkeypatch.setattr(run.beam.io, "ReadFromPubSub", mock_read_from_pubsub)
    monkeypatch.setattr(
        run.transforms, "KlioReadFromText", mock_read_from_file
    )
    monkeypatch.setattr(run.transforms, "KlioWriteToText", mock_write_to_file)
    monkeypatch.setattr(run.beam.io, "WriteToPubSub", mock_write_to_pubsub)
    monkeypatch.setattr(
        run.BatchEventMapper, "input", {"file": mock_read_from_file},
    )
    monkeypatch.setattr(
        run.BatchEventMapper, "output", {"file": mock_write_to_file},
    )
    config.pipeline_options.streaming = False

    mock_pipeline.return_value.run.side_effect = ValueError(value_err_msg)

    kpipe = run.KlioPipeline(job_name, config, mock_runtime_config)
    with pytest.raises(SystemExit):
        kpipe.run()

    mock_pipeline.return_value.run.assert_called_once_with()
    assert 1 == len(caplog.records)
    assert "ERROR" == caplog.records[0].levelname

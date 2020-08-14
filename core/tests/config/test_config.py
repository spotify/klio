# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import pytest

from klio_core import config
from klio_core.config import _io as io


@pytest.fixture
def job_config_dict():
    return {
        "metrics": {"logger": {}},
        "inputs": [
            {
                "topic": "test-parent-job-out",
                "subscription": "test-parent-job-out-sub",
                "data_location": "gs://sigint-output/test-parent-job-out",
            }
        ],
        "outputs": [
            {
                "topic": "test-job-out",
                "data_location": "gs://sigint-output/test-job-out",
            }
        ],
        "dependencies": [
            {"gcp_project": "sigint", "job_name": "test-parent-job"}
        ],
        "thread_pool_processes": 3,
        "number_of_retries": 3,
        "more": "config",
        "that": {"the": "user"},
        "might": ["include"],
        "blocking": False,
        "timeout_threshold": 0,
        "allow_non_klio_messages": False,
        "binary_non_klio_messages": False,
    }


@pytest.fixture
def job_v2_config_dict():
    return {
        "metrics": {"logger": {}},
        "events": {
            "inputs": [
                {
                    "type": "pubsub",
                    "topic": "test-parent-job-out",
                    "subscription": "test-parent-job-out-sub",
                },
            ],
            "outputs": [{"type": "pubsub", "topic": "test-job-out"}],
        },
        "data": {
            "inputs": [
                {
                    "type": "GCS",
                    "location": "gs://sigint-output/test-parent-job-out",
                }
            ],
            "outputs": [
                {"type": "GCS", "location": "gs://sigint-output/test-job-out"}
            ],
        },
        "dependencies": [
            {"gcp_project": "sigint", "job_name": "test-parent-job"}
        ],
        "thread_pool_processes": 3,
        "number_of_retries": 3,
        "more": "config",
        "that": {"the": "user"},
        "might": ["include"],
        "blocking": False,
    }


@pytest.fixture
def pipeline_config_dict():
    return {
        "project": "test-project",
        "staging_location": "gs://some/stage",
        "temp_location": "gs://some/temp",
        "worker_harness_container_image": "gcr.io/sigint/foo",
        "streaming": True,
        "update": False,
        "experiments": ["beam_fn_api"],
        "region": "us-central1",
        "num_workers": 3,
        "random_param": "wombat",
        "max_num_workers": 5,
        "disk_size_gb": 50,
        "worker_machine_type": "n1-standard-4",
        "dataflow_endpoint": "https://example.com",
        "service_account_email": "sudo@example.com",
        "no_auth": True,
        "template_location": "gs://some/template",
        "labels": ["some", "labels"],
        "label": "single_label",
        "transform_name_mapping": '{"transform": "is mapped", "using": "json"}',
        "dataflow_kms_key": "a_key_name",
        "autoscaling_algorithm": "THROUGHPUT_BASED",
        "flexrs_goal": "COST_OPTIMIZED",
        "worker_disk_type": "pd-ssd",
        "use_public_ips": True,
        "min_cpu_platform": "Intel Skylake",
        "dataflow_worker_jar": "/foo/bar.jar",
        "subnetwork": "https://www.googleapis.com/compute/v1/projects/"
        "test-project/regions/us-central1/subnetworks/xpn-us1",
    }


@pytest.fixture
def final_pipeline_config_dict():
    return {
        "project": "test-project",
        "staging_location": "gs://some/stage",
        "temp_location": "gs://some/temp",
        "worker_harness_container_image": "gcr.io/sigint/foo",
        "streaming": True,
        "update": False,
        "experiments": ["beam_fn_api"],
        "region": "us-central1",
        "num_workers": 3,
        "random_param": "wombat",
        "max_num_workers": 5,
        "disk_size_gb": 50,
        "worker_machine_type": "n1-standard-4",
        "dataflow_endpoint": "https://example.com",
        "service_account_email": "sudo@example.com",
        "no_auth": True,
        "template_location": "gs://some/template",
        "labels": ["some", "labels", "single_label"],
        "label": "single_label",
        "transform_name_mapping": '{"transform": "is mapped", "using": "json"}',
        "dataflow_kms_key": "a_key_name",
        "flexrs_goal": "COST_OPTIMIZED",
        "worker_disk_type": "compute.googleapis.com/projects/"
        "test-project/regions/us-central1/diskTypes/pd-ssd",
        "use_public_ips": True,
        "min_cpu_platform": "Intel Skylake",
        "dataflow_worker_jar": "/foo/bar.jar",
        "job_name": "test-job",
        "runner": "DataflowRunner",
        "subnetwork": "https://www.googleapis.com/compute/v1/projects/"
        "test-project/regions/us-central1/subnetworks/xpn-us1",
        "enable_streaming_engine": False,
        "autoscaling_algorithm": "THROUGHPUT_BASED",
        "requirements_file": None,
        "sdk_location": None,
        "setup_file": None,
        "profile_location": None,
        "profile_cpu": None,
        "profile_memory": None,
        "profile_sample_rate": None,
    }


@pytest.fixture
def empty_pipeline_config_dict():
    return {
        "project": "test-project",
        "staging_location": "gs://some/stage",
        "temp_location": "gs://some/temp",
        "worker_harness_container_image": "gcr.io/sigint/foo",
        "streaming": True,
        "update": False,
        "experiments": ["beam_fn_api"],
        "region": "us-central1",
        "num_workers": 3,
        "max_num_workers": 5,
        "disk_size_gb": 50,
        "worker_machine_type": "n1-standard-4",
    }


@pytest.fixture
def bare_pipeline_config_dict():
    return {
        "streaming": True,
        "experiments": ["beam_fn_api"],
        "runner": "direct",
    }


@pytest.fixture
def config_dict(job_config_dict, pipeline_config_dict):
    return {
        "job_config": job_config_dict,
        "pipeline_options": pipeline_config_dict,
        "job_name": "test-job",
    }


@pytest.fixture
def final_config_dict(final_pipeline_config_dict, job_config_dict):
    return {
        "version": 1,
        "job_name": "test-job",
        "pipeline_options": final_pipeline_config_dict,
        "job_config": job_config_dict,
    }


@pytest.fixture
def no_gcp_config_dict(job_config_dict, empty_pipeline_config_dict):
    return {
        "job_config": job_config_dict,
        "pipeline_options": empty_pipeline_config_dict,
        "job_name": "test-job",
    }


@pytest.mark.parametrize("blocking", (True, False, None))
@pytest.mark.parametrize(
    "dependencies,timeout_threshold",
    (
        (True, 0),
        (False, 0),
        (True, 30),
        (False, 30),
        (True, None),
        (False, None),
    ),
)
def test_klio_job_config(
    job_config_dict, dependencies, timeout_threshold, blocking
):
    if blocking is None:
        job_config_dict.pop("blocking")
    else:
        job_config_dict["blocking"] = blocking

    if not dependencies:
        job_config_dict.pop("dependencies")

    if not timeout_threshold:
        timeout_threshold = 0
    else:
        job_config_dict["timeout_threshold"] = timeout_threshold

    config_obj = config.KlioJobConfig(
        job_config_dict, job_name="test-job", version=1
    )

    assert {"logger": {}} == config_obj.metrics
    assert 3 == config_obj.thread_pool_processes
    assert timeout_threshold == config_obj.timeout_threshold
    assert 3 == config_obj.number_of_retries

    ret_input_topics = [i.topic for i in config_obj.event_inputs]
    ret_output_topics = [o.topic for o in config_obj.event_outputs]
    ret_input_topics = [i.topic for i in config_obj.inputs]
    ret_output_topics = [o.topic for o in config_obj.outputs]

    assert ["test-parent-job-out"] == ret_input_topics
    assert ["test-job-out"] == ret_output_topics

    ret_input_data = [i.location for i in config_obj.data_inputs]
    ret_output_data = [o.location for o in config_obj.data_outputs]
    ret_input_data = [i.data_location for i in config_obj.inputs]
    ret_output_data = [o.data_location for o in config_obj.outputs]

    assert ["gs://sigint-output/test-parent-job-out"] == ret_input_data
    assert ["gs://sigint-output/test-job-out"] == ret_output_data
    if dependencies:
        assert [
            {"gcp_project": "sigint", "job_name": "test-parent-job"}
        ] == config_obj.dependencies

    if blocking:
        assert config_obj.blocking is True
    else:
        assert config_obj.blocking is False

    if not dependencies:
        job_config_dict["dependencies"] = []
    if blocking is None:
        job_config_dict["blocking"] = False

    assert job_config_dict == config_obj.as_dict()

    repr_actual = repr(config_obj)
    assert "KlioJobConfig(job_name='test-job')" == repr_actual


def test_v2_job_config(job_v2_config_dict):
    config_obj = config.KlioJobConfig(
        job_v2_config_dict, job_name="test-job", version=2
    )

    assert {"logger": {}} == config_obj.metrics
    assert 3 == config_obj.thread_pool_processes
    assert 0 == config_obj.timeout_threshold
    assert 3 == config_obj.number_of_retries

    ret_input_topics = [i.topic for i in config_obj.event_inputs]
    ret_output_topics = [o.topic for o in config_obj.event_outputs]

    assert ["test-parent-job-out"] == ret_input_topics
    assert ["test-job-out"] == ret_output_topics

    ret_input_data = [i.location for i in config_obj.data_inputs]
    ret_output_data = [o.location for o in config_obj.data_outputs]

    assert ["gs://sigint-output/test-parent-job-out"] == ret_input_data
    assert ["gs://sigint-output/test-job-out"] == ret_output_data

    repr_actual = repr(config_obj)
    assert "KlioJobConfig(job_name='test-job')" == repr_actual


def test_bare_klio_pipeline_config(bare_pipeline_config_dict):
    config_obj = config.KlioPipelineConfig(
        bare_pipeline_config_dict, version=1, job_name="test-job"
    )

    assert config_obj.streaming is True
    assert not config_obj.update
    assert ["beam_fn_api"] == config_obj.experiments
    assert "direct" == config_obj.runner
    assert "europe-west1" == config_obj.region
    assert config_obj.subnetwork is None
    assert 2 == config_obj.num_workers
    assert 2 == config_obj.max_num_workers
    assert 32 == config_obj.disk_size_gb
    assert "n1-standard-2" == config_obj.worker_machine_type
    assert config_obj.no_auth is False
    assert [] == config_obj.labels
    assert config_obj.enable_streaming_engine is False
    assert config_obj.autoscaling_algorithm == "NONE"

    expected_none_attrs = [
        "project",
        "staging_location",
        "temp_location",
        "worker_harness_container_image",
        "sdk_location",
        "setup_file",
        "requirements_file",
        "dataflow_endpoint",
        "service_account_email",
        "template_location",
        "transform_name_mapping",
        "dataflow_kms_key",
        "flexrs_goal",
        "worker_disk_type",
        "use_public_ips",
        "min_cpu_platform",
        "dataflow_worker_jar",
    ]
    for attr in expected_none_attrs:
        attr_to_test = getattr(config_obj, attr)
        assert attr_to_test is None

    repr_actual = repr(config_obj)
    assert "KlioPipelineConfig(job_name='test-job')" == repr_actual


def test_klio_pipeline_config(
    pipeline_config_dict, final_pipeline_config_dict,
):

    config_obj = config.KlioPipelineConfig(
        pipeline_config_dict, job_name="test-job", version=1
    )

    config_sub_network = (
        "https://www.googleapis.com/compute/v1/projects/test-project/"
        "regions/us-central1/subnetworks/xpn-us1"
    )

    assert "test-project" == config_obj.project
    assert "gs://some/stage" == config_obj.staging_location
    assert "gs://some/temp" == config_obj.temp_location
    assert "gcr.io/sigint/foo" == config_obj.worker_harness_container_image
    assert config_obj.streaming
    assert not config_obj.update
    assert ["beam_fn_api"] == config_obj.experiments
    assert "us-central1" == config_obj.region
    assert config_sub_network == config_obj.subnetwork
    assert 3 == config_obj.num_workers
    assert 5 == config_obj.max_num_workers
    assert 50 == config_obj.disk_size_gb
    assert "n1-standard-4" == config_obj.worker_machine_type
    assert config_obj.sdk_location is None
    assert "DataflowRunner" == config_obj.runner
    assert "https://example.com" == config_obj.dataflow_endpoint
    assert "sudo@example.com" == config_obj.service_account_email
    assert config_obj.no_auth is True
    assert "gs://some/template" == config_obj.template_location
    assert ["some", "labels", "single_label"] == config_obj.labels
    assert "single_label" == config_obj.label
    assert (
        '{"transform": "is mapped", "using": "json"}'
        == config_obj.transform_name_mapping
    )
    assert "THROUGHPUT_BASED" == config_obj.autoscaling_algorithm
    assert "COST_OPTIMIZED" == config_obj.flexrs_goal
    assert (
        "compute.googleapis.com/projects/test-project/regions/us-central1/"
        "diskTypes/pd-ssd" == config_obj.worker_disk_type
    )
    assert config_obj.use_public_ips is True
    assert "Intel Skylake" == config_obj.min_cpu_platform
    assert "/foo/bar.jar" == config_obj.dataflow_worker_jar
    assert "wombat" == config_obj.random_param

    assert final_pipeline_config_dict == config_obj.as_dict()

    repr_actual = repr(config_obj)
    assert "KlioPipelineConfig(job_name='test-job')" == repr_actual


def test_klio_config(config_dict, final_config_dict):

    config_obj = config.KlioConfig(config_dict)

    assert "test-job" == config_obj.job_name
    assert isinstance(config_obj.job_config, config.KlioJobConfig)
    assert isinstance(config_obj.pipeline_options, config.KlioPipelineConfig)
    assert final_config_dict == config_obj.as_dict()

    repr_actual = repr(config_obj)
    assert "KlioConfig(job_name='test-job')" == repr_actual


def test_no_gcp_klio_config(no_gcp_config_dict):

    config_obj = config.KlioConfig(no_gcp_config_dict)

    assert "test-job" == config_obj.job_name
    assert isinstance(config_obj.job_config, config.KlioJobConfig)
    assert isinstance(config_obj.pipeline_options, config.KlioPipelineConfig)
    # Default variables are added to the pipeline config
    assert config_dict != config_obj.as_dict()

    repr_actual = repr(config_obj)
    assert "KlioConfig(job_name='test-job')" == repr_actual


def test_klio_read_file_config():
    config_dict = {
        "type": "GCS",
        "location": "gs://sigint-output/test-parent-job-out",
    }
    klio_read_file_config = io.KlioReadFileConfig.from_dict(
        config_dict, io.KlioIOType.DATA, io.KlioIODirection.INPUT
    )

    assert "file" == klio_read_file_config.name
    assert config_dict["location"] == klio_read_file_config.file_pattern


def test_klio_write_file_config():
    config_dict = {
        "type": "GCS",
        "location": "gs://sigint-output/test-parent-job-out",
    }
    klio_write_file_config = io.KlioWriteFileConfig.from_dict(
        config_dict, io.KlioIOType.DATA, io.KlioIODirection.OUTPUT
    )

    assert "file" == klio_write_file_config.name
    assert config_dict["location"] == klio_write_file_config.file_path_prefix


def test_klio_write_bigquery_config():
    config_dict = {
        "type": "bq",
        "project": "a-project",
        "dataset": "a-dataset",
        "table": "a-table",
        "create_disposition": "CREATE_IF_NEEDED",
        "write_disposition": "WRITE_TRUNCATE",
        "schema": {
            "fields": [{"name": "label", "type": "STRING", "mode": "nullable"}]
        },
    }

    klio_write_bq_cfg = io.KlioBigQueryEventOutput.from_dict(
        config_dict, io.KlioIOType.EVENT, io.KlioIODirection.OUTPUT
    )

    assert "bq" == klio_write_bq_cfg.name
    assert config_dict["schema"] == klio_write_bq_cfg.schema
    assert (
        config_dict["create_disposition"]
        == klio_write_bq_cfg.create_disposition
    )
    assert (
        config_dict["write_disposition"] == klio_write_bq_cfg.write_disposition
    )


@pytest.mark.parametrize(
    "schema",
    (
        {},
        {"not-fields": []},
        {"fields": None},
        {"fields": [{}]},
        {"fields": [{"name": "a", "type": "b"}]},
        {"fields": [{"name": "a", "type": "b", "mode": "c"}, {}]},
    ),
)
def test_klio_write_bigquery_config_raises(schema):
    config_dict = {
        "type": "bq",
        "project": "a-project",
        "dataset": "a-dataset",
        "table": "a-table",
        "create_disposition": "CREATE_IF_NEEDED",
        "write_disposition": "WRITE_TRUNCATE",
        "schema": schema,
    }

    with pytest.raises(ValueError):
        io.KlioBigQueryEventOutput.from_dict(
            config_dict, io.KlioIOType.EVENT, io.KlioIODirection.OUTPUT
        )


@pytest.mark.parametrize(
    "include_topic,include_subscription",
    ((False, True), (True, False), (True, True),),
)
def test_pubsub_event_input_kwargs(include_topic, include_subscription):
    config_dict = {
        "type": "pubsub",
        "topic": "a-topic",
        "subscription": "a-subscription",
    }

    if not include_topic:
        config_dict.pop("topic")
    if not include_subscription:
        config_dict.pop("subscription")

    pubsub = io.KlioPubSubEventInput.from_dict(
        config_dict, io.KlioIOType.EVENT, io.KlioIODirection.INPUT
    )

    if include_subscription:
        expected = {
            "subscription": "a-subscription",
        }
    else:
        expected = {
            "topic": "a-topic",
        }

    assert expected == pubsub.to_io_kwargs()


def test_pubsub_event_input_topic_subscription():
    config_dict = {"type": "pubsub"}

    with pytest.raises(ValueError):
        io.KlioPubSubEventInput.from_dict(
            config_dict, io.KlioIOType.EVENT, io.KlioIODirection.INPUT
        )

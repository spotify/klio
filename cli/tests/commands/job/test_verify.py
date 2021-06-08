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

import logging

import httplib2
import pytest

from google.api_core import exceptions as api_ex
from google.api_core import page_iterator
from google.cloud import exceptions
from googleapiclient import errors as google_errors

from klio_core import config as kconfig

from klio_cli.commands.job import verify
from klio_cli.utils import stackdriver_utils as sd_utils


@pytest.fixture
def mock_publisher(mocker):
    return mocker.patch.object(verify.pubsub_v1, "PublisherClient")


@pytest.fixture
def mock_sub(mocker):
    return mocker.patch.object(verify.pubsub_v1, "SubscriberClient")


@pytest.fixture
def mock_storage(mocker):
    return mocker.patch.object(verify.storage, "Client")


@pytest.fixture
def mock_bucket(mocker):
    return mocker.patch.object(verify.storage.Client, "bucket")


@pytest.fixture
def mock_blob(mocker):
    return mocker.patch.object(verify.storage, "blob")


@pytest.fixture
def mock_discovery_client(mocker, monkeypatch):
    mock = mocker.MagicMock()
    monkeypatch.setattr(verify.discovery, "build", lambda x, y: mock)
    return mock


@pytest.fixture
def mock_iterator(mocker):
    return mocker.patch.object(page_iterator, "HTTPIterator")


@pytest.fixture
def config():
    return {
        "job_name": "klio-job-name",
        "job_config": {
            "inputs": [
                {
                    "topic": "test-in-topic",
                    "subscription": "foo",
                    "data_location": "gs://test-in-data",
                }
            ],
            "outputs": [
                {
                    "topic": "test-out-topic",
                    "data_location": "gs://test-out-data",
                }
            ],
        },
        "pipeline_options": {
            "streaming": True,
            "worker_harness_container_image": "a-worker-image",
            "experiments": ["beam_fn_api"],
            "project": "test-gcp-project",
            "zone": "europe-west1-c",
            "region": "europe-west1",
            "staging_location": "gs://test-gcp-project-dataflow-tmp/staging",
            "temp_location": "gs://test-gcp-project-dataflow-tmp/temp",
            "max_num_workers": 2,
            "autoscaling_algorithm": "NONE",
            "disk_size_gb": 32,
            "worker_machine_type": "n1-standard-2",
            "runner": "DataflowRunner",
        },
    }


@pytest.fixture
def klio_config(config):
    return kconfig.KlioConfig(config)


@pytest.fixture
def mock_get_sd_group_url(mocker):
    return mocker.patch.object(sd_utils, "get_stackdriver_group_url")


@pytest.fixture
def mock_create_sd_group(mocker):
    return mocker.patch.object(sd_utils, "create_stackdriver_group")


@pytest.mark.parametrize(
    "expected,bindings,create_resources",
    [
        (
            True,
            [
                {
                    "role": "roles/monitoring.metricWriter",
                    "members": ["serviceAccount:the-default-svc-account"],
                }
            ],
            False,
        ),
        (
            False,
            [
                {
                    "role": "roles/monitoring.metricWriter",
                    "members": ["serviceAccount:some-other-account"],
                }
            ],
            False,
        ),
        (
            False,
            [
                {
                    "role": "roles/monitoring.metricWriter",
                    "members": ["serviceAccount:some-other-account"],
                }
            ],
            True,
        ),
        (
            False,
            [{"role": "roles/monitoring.metricWriter", "members": []}],
            False,
        ),
        (False, [], False),
    ],
)
def test_verify_iam_roles(
    caplog,
    expected,
    bindings,
    create_resources,
    mock_discovery_client,
    klio_config,
):

    compute_client = mock_discovery_client.build("compute")
    compute_client.projects().get().execute.return_value = {
        "defaultServiceAccount": "the-default-svc-account"
    }
    iam_client = mock_discovery_client.build("cloudresourcemanager")

    job = verify.VerifyJob(klio_config, create_resources)
    job._compute_client = compute_client
    job._iam_client = iam_client

    gcp_project = job.klio_config.pipeline_options.project
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.return_value = {"bindings": bindings}
    result = job._verify_iam_roles()
    if create_resources:
        assert (
            "--create-resources is not able to add these roles"
            in caplog.records[-1].msg
        )

    compute_client.projects().get(
        project=gcp_project
    ).execute.assert_called_once_with()
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.assert_called_once_with()
    assert result is expected


def test_verify_iam_roles_editor(caplog, klio_config, mock_discovery_client):
    bindings = [
        {
            "role": "roles/monitoring.metricWriter",
            "members": ["serviceAccount:the-default-svc-account"],
        },
        {
            "role": "roles/editor",
            "members": ["serviceAccount:the-default-svc-account"],
        },
    ]
    gcp_project = klio_config.pipeline_options.project
    compute_client = mock_discovery_client.build("compute")
    compute_client.projects().get().execute.return_value = {
        "defaultServiceAccount": "the-default-svc-account"
    }
    iam_client = mock_discovery_client.build("cloudresourcemanager")
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.return_value = {"bindings": bindings}

    job = verify.VerifyJob(klio_config, False)
    job._compute_client = compute_client
    job._iam_client = iam_client
    result = job._verify_iam_roles()

    compute_client.projects().get(
        project=gcp_project
    ).execute.assert_called_once_with()
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.assert_called_once_with()
    assert result is True
    with caplog.at_level(logging.WARNING):
        assert len(caplog.records) == 3
        msg = caplog.records[1].msg
        assert "unsafe project editor or owner permissions" in msg


def test_verify_iam_roles_with_svc_account(klio_config, mock_discovery_client):
    "If the user configures a SA, verify it instead of the default compute SA"
    job = verify.VerifyJob(klio_config, False)
    job.klio_config.pipeline_options.service_account_email = (
        "my.sa@something.com"
    )
    bindings = [
        {
            "role": "roles/monitoring.metricWriter",
            "members": ["serviceAccount:my.sa@something.com"],
        },
        {
            "role": "roles/editor",
            "members": ["serviceAccount:the-default-svc-account"],
        },
    ]
    gcp_project = job.klio_config.pipeline_options.project
    compute_client = mock_discovery_client.build("compute")
    compute_client.projects().get().execute.return_value = {
        "defaultServiceAccount": "the-default-svc-account"
    }
    iam_client = mock_discovery_client.build("cloudresourcemanager")
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.return_value = {"bindings": bindings}
    job._compute_client = compute_client
    job._iam_client = iam_client

    result = job._verify_iam_roles()

    # Assert that we don't fetch the default SA since we don't need it
    compute_client.projects().get(
        project=gcp_project
    ).execute.assert_not_called()
    iam_client.projects().getIamPolicy(
        resource=gcp_project, body={}
    ).execute.assert_called_once_with()
    assert result is True


def test_verify_iam_roles_http_error(klio_config, mock_discovery_client):
    compute_client = mock_discovery_client.build("compute")
    err = google_errors.HttpError
    resp = httplib2.Response({})
    resp.reason = "some resp"
    compute_client.projects().get().execute.side_effect = err(
        resp, "some content".encode()
    )
    iam_client = mock_discovery_client.build("cloudresourcemanager")
    job = verify.VerifyJob(klio_config, False)
    job._compute_client = compute_client
    job._iam_client = iam_client
    result = job._verify_iam_roles()
    assert result is False

    compute_client = mock_discovery_client.build("compute")
    compute_client.projects().get().execute.side_effect = None
    compute_client.projects().get().execute.return_value = {
        "defaultServiceAccount": "the-default-svc-account"
    }
    iam_client = mock_discovery_client.build("cloudresourcemanager")
    iam_client.projects().getIamPolicy(
        resource=job.klio_config.pipeline_options.project, body={}
    ).execute.side_effect = google_errors.HttpError(
        resp, "some content".encode()
    )
    job._compute_client = compute_client
    job._iam_client = iam_client
    result = job._verify_iam_roles()
    assert result is False


@pytest.mark.parametrize(
    "create_resources,dashboard_url",
    ((True, None), (False, None), (False, "dashboard/url")),
)
def test_verify_stackdriver_dashboard(
    klio_config,
    mock_get_sd_group_url,
    mock_create_sd_group,
    create_resources,
    dashboard_url,
):
    mock_get_sd_group_url.return_value = dashboard_url

    job = verify.VerifyJob(klio_config, create_resources)
    actual = job._verify_stackdriver_dashboard()

    mock_get_sd_group_url.assert_called_once_with(
        "test-gcp-project", "klio-job-name", "europe-west1"
    )
    if create_resources:
        mock_create_sd_group.assert_called_once_with(
            "test-gcp-project", "klio-job-name", "europe-west1"
        )
        assert actual is True
    elif dashboard_url:
        assert actual is True
    else:
        assert actual is False


def test_verify_stackdriver_dashboard_raises(
    klio_config, mock_get_sd_group_url, caplog
):
    mock_get_sd_group_url.side_effect = Exception("error")

    with pytest.raises(Exception, match="error"):
        job = verify.VerifyJob(klio_config, False)
        job._verify_stackdriver_dashboard()

    assert 2 == len(caplog.records)


def test_verify_stackdriver_dashboard_errors(
    klio_config, mock_get_sd_group_url, mock_create_sd_group, caplog
):
    mock_get_sd_group_url.return_value = None
    mock_create_sd_group.return_value = None

    job = verify.VerifyJob(klio_config, True)
    actual = job._verify_stackdriver_dashboard()

    assert actual is False
    assert 3 == len(caplog.records)


@pytest.mark.parametrize("create_resources", (True, False))
def test_verify_gcs_bucket(klio_config, mock_storage, create_resources):
    test_path = "gs://bucket/blob"

    job = verify.VerifyJob(klio_config, create_resources)
    job._storage_client = mock_storage
    actual = job._verify_gcs_bucket(test_path)

    if create_resources:
        mock_storage.create_bucket.assert_called_once_with("bucket")
    else:
        mock_storage.get_bucket.assert_called_once_with("bucket")

    assert actual is True


def test_verify_gcs_bucket_invalid_name(klio_config, mock_storage, caplog):
    job = verify.VerifyJob(klio_config, True)
    job._storage_client = mock_storage
    assert not job._verify_gcs_bucket("a/b/c")
    assert 2 == len(caplog.records)


def test_verify_gcs_bucket_exists(klio_config, mock_storage):
    test_path = "gs://bucket/blob"
    mock_storage.create_bucket.side_effect = api_ex.Conflict("test")

    job = verify.VerifyJob(klio_config, True)
    job._storage_client = mock_storage
    actual = job._verify_gcs_bucket(test_path)

    assert actual is True


@pytest.mark.parametrize("not_found", (True, False))
def test_verify_gcs_bucket_exceptions(klio_config, mock_storage, not_found):
    test_path = "gs://bucket/blob"

    if not_found:
        mock_storage.get_bucket.side_effect = exceptions.NotFound("test")
    else:
        mock_storage.get_bucket.side_effect = Exception

    job = verify.VerifyJob(klio_config, False)
    job._storage_client = mock_storage
    actual = job._verify_gcs_bucket(test_path)

    assert actual is False


@pytest.mark.parametrize("create_resources", (True, False))
def test_verify_pub_topic(klio_config, mock_publisher, create_resources):
    test_topic = "test"

    job = verify.VerifyJob(klio_config, create_resources)
    job._publisher_client = mock_publisher
    actual = job._verify_pub_topic(test_topic, input)

    if create_resources:
        mock_publisher.create_topic.assert_called_once_with(
            request={"name": test_topic}
        )
    else:
        mock_publisher.get_topic.assert_called_once_with(
            request={"topic": test_topic}
        )

    assert actual is True


def test_verify_pub_topic_exists(
    klio_config, mock_publisher,
):
    test_topic = "test"
    mock_publisher.create_topic.side_effect = api_ex.AlreadyExists("test")

    job = verify.VerifyJob(klio_config, True)
    job._publisher_client = mock_publisher
    actual = job._verify_pub_topic(test_topic, input)

    assert actual is True


@pytest.mark.parametrize("not_found", (True, False))
def test_verify_pub_topic_exceptions(klio_config, mock_publisher, not_found):
    test_topic = "test"

    if not_found:
        mock_publisher.get_topic.side_effect = exceptions.NotFound("test")
    else:
        mock_publisher.get_topic.side_effect = Exception

    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    actual = job._verify_pub_topic(test_topic, input)

    assert actual is False


@pytest.mark.parametrize("create_resources", (True, False))
def test_verify_subscription_and_topic(
    klio_config, mock_publisher, mock_sub, create_resources
):
    test_sub = "test"
    upstream_topic = "Some"
    job = verify.VerifyJob(klio_config, create_resources)
    job._publisher_client = mock_publisher
    job._subscriber_client = mock_sub

    if create_resources:
        actual = job._verify_subscription_and_topic(test_sub, upstream_topic,)
        mock_sub.create_subscription.assert_called_once_with(
            request={"name": test_sub, "topic": upstream_topic}
        )
    else:
        actual = job._verify_subscription_and_topic(test_sub, upstream_topic,)
        mock_sub.get_subscription.assert_called_once_with(
            request={"subscription": test_sub}
        )

    expected = True, True

    assert expected == actual


def test_verify_subscription_and_topic_exists(
    klio_config, mock_publisher, mock_sub
):
    test_sub = "test"
    upstream_topic = "Some"
    job = verify.VerifyJob(klio_config, True)
    job._publisher_client = mock_publisher
    job._subscriber_client = mock_sub

    mock_sub.create_subscription.side_effect = api_ex.AlreadyExists("test")
    actual = job._verify_subscription_and_topic(test_sub, upstream_topic)

    expected = True, True

    assert expected == actual


@pytest.mark.parametrize(
    "not_found, no_topic", ((True, False), (False, False), (False, True))
)
def test_verify_subscription_and_topic_exceptions(
    klio_config, mock_publisher, mock_sub, not_found, no_topic
):
    test_sub = "test"
    if no_topic:
        upstream_topic = None
        expected = False, True
    else:
        upstream_topic = "Some"
        expected = True, False

    if not_found:
        mock_sub.get_subscription.side_effect = exceptions.NotFound("test")
    else:
        mock_sub.get_subscription.side_effect = Exception

    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._subscriber_client = mock_sub
    actual = job._verify_subscription_and_topic(test_sub, upstream_topic)

    assert expected == actual


mock_gcs_event_config = {
    "type": "gcs",
    "location": "gs://some/events.txt",
    "io_type": kconfig._io.KlioIOType.EVENT,
    "io_direction": kconfig._io.KlioIODirection.INPUT,
}
mock_gcs_event = kconfig._io.KlioReadFileConfig.from_dict(
    mock_gcs_event_config
)

mock_bq_event_config = {
    "type": "bq",
    "project": "sigint",
    "dataset": "test-data",
    "table": "test-table",
    "io_type": kconfig._io.KlioIOType.EVENT,
    "io_direction": kconfig._io.KlioIODirection.INPUT,
}
mock_bq_event = kconfig._io.KlioBigQueryEventInput.from_dict(
    mock_bq_event_config
)

mock_avro_event_config = {
    "type": "avro",
    "io_type": kconfig._io.KlioIOType.EVENT,
    "io_direction": kconfig._io.KlioIODirection.INPUT,
}
mock_avro_event = kconfig._io.KlioReadAvroEventConfig.from_dict(
    mock_avro_event_config
)


@pytest.mark.parametrize(
    "mock_event_input", ((mock_gcs_event), (mock_bq_event), (mock_avro_event),)
)
def test_unverified_event_inputs(
    mocker,
    caplog,
    mock_storage,
    mock_publisher,
    mock_sub,
    klio_config,
    mock_event_input,
):
    mock_verify_gcs_bucket = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )
    mock_verify_sub = mocker.patch.object(
        verify.VerifyJob, "_verify_subscription_and_topic"
    )
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage
    job._subscriber_client = mock_sub
    job.klio_config.pipeline_options.project = "sigint"

    job.klio_config.job_config.events.inputs = [mock_event_input]

    data_config = {
        "type": "gcs",
        "location": "test",
        "io_type": kconfig._io.KlioIOType.DATA,
        "io_direction": kconfig._io.KlioIODirection.INPUT,
    }
    job.klio_config.job_config.data.inputs = [
        kconfig._io.KlioGCSInputDataConfig.from_dict(data_config)
    ]
    mock_verify_gcs_bucket.return_value = True

    actual = job._verify_inputs()

    mock_verify_sub.assert_not_called()
    mock_verify_gcs_bucket.assert_called_with("test")
    assert actual
    assert 3 == len(caplog.records)


@pytest.mark.parametrize(
    "unverified_bucket, unverified_topic, unverified_sub",
    (
        (False, False, False),
        (True, False, False),
        (True, True, True),
        (False, True, True),
        (False, False, True),
        (False, True, False),
        (False, False, False),
    ),
)
def test_verify_inputs(
    mocker,
    unverified_bucket,
    unverified_topic,
    unverified_sub,
    klio_config,
    mock_storage,
    mock_publisher,
    mock_sub,
):
    mock_verify_gcs_bucket = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )
    mock_verify_sub = mocker.patch.object(
        verify.VerifyJob, "_verify_subscription_and_topic"
    )
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage
    job._subscriber_client = mock_sub
    job.klio_config.pipeline_options.project = "sigint"

    event_config = {
        "type": "pubsub",
        "topic": "test",
        "subscription": "test",
        "io_type": kconfig._io.KlioIOType.EVENT,
        "io_direction": kconfig._io.KlioIODirection.INPUT,
    }
    job.klio_config.job_config.events.inputs = [
        kconfig._io.KlioPubSubEventInput.from_dict(event_config)
    ]

    data_config = {
        "type": "gcs",
        "location": "test",
        "io_type": kconfig._io.KlioIOType.DATA,
        "io_direction": kconfig._io.KlioIODirection.INPUT,
    }
    job.klio_config.job_config.data.inputs = [
        kconfig._io.KlioGCSInputDataConfig.from_dict(data_config)
    ]

    if unverified_topic and unverified_sub and unverified_bucket:
        mock_verify_gcs_bucket.return_value = False
        mock_verify_sub.return_value = False, False
        actual = job._verify_inputs()
        expected = False
        assert expected == actual
    elif unverified_topic and unverified_sub:
        mock_verify_sub.return_value = False, False
        actual = job._verify_inputs()
        expected = False
        assert expected == actual
    elif unverified_topic:
        mock_verify_sub.return_value = False, True
        actual = job._verify_inputs()
        expected = False
        assert expected == actual
    elif unverified_sub:
        mock_verify_sub.return_value = True, False
        actual = job._verify_inputs()
        expected = False
        assert expected == actual
    elif unverified_bucket:
        mock_verify_gcs_bucket.return_value = False
        mock_verify_sub.return_value = True, True
        actual = job._verify_inputs()
        expected = False
        assert expected == actual
    else:
        mock_verify_gcs_bucket.return_value = True
        mock_verify_sub.return_value = True, True
        actual = job._verify_inputs()
        expected = True
        assert expected == actual

    mock_verify_gcs_bucket.assert_called_with("test")
    mock_verify_sub.assert_called_with("test", "test")


@pytest.mark.parametrize(
    "data_dict, event_dict, expected_log_count",
    (
        ({"location": "test"}, {"topic": "test", "subscription": "test"}, 3),
        ({"location": None}, {"topic": "test", "subscription": "test"}, 4),
        ({"location": "test"}, {"topic": "test", "subscription": None}, 4),
        ({"location": None}, {"topic": "test", "subscription": None}, 5),
        (None, None, 5),
    ),
)
def test_verify_inputs_logs(
    data_dict,
    event_dict,
    expected_log_count,
    klio_config,
    mock_storage,
    mock_publisher,
    mock_sub,
    mocker,
    caplog,
):
    mocker.patch.object(verify.VerifyJob, "_verify_gcs_bucket")
    mock_verify_sub = mocker.patch.object(
        verify.VerifyJob, "_verify_subscription_and_topic"
    )
    mock_verify_sub.return_value = (False, False)

    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage
    job._subscriber_client = mock_sub
    job.klio_config.pipeline_options.project = "sigint"

    if event_dict:
        event_dict["type"] = "pubsub"
        event_dict["io_type"] = kconfig._io.KlioIOType.EVENT
        event_dict["io_direction"] = kconfig._io.KlioIODirection.OUTPUT
        job.klio_config.job_config.events.inputs = [
            kconfig._io.KlioPubSubEventInput.from_dict(event_dict)
        ]
    else:
        job.klio_config.job_config.events.inputs = []

    if data_dict:
        data_dict["type"] = "gcs"
        data_dict["io_type"] = kconfig._io.KlioIOType.DATA
        data_dict["io_direction"] = kconfig._io.KlioIODirection.OUTPUT
        job.klio_config.job_config.data.inputs = [
            kconfig._io.KlioGCSOutputDataConfig.from_dict(data_dict)
        ]
    else:
        job.klio_config.job_config.data.inputs = []

    job._verify_inputs()
    assert expected_log_count == len(caplog.records)


mock_gcs_output_event_config = {
    "type": "gcs",
    "location": "gs://some/events.txt",
    "io_type": kconfig._io.KlioIOType.EVENT,
    "io_direction": kconfig._io.KlioIODirection.OUTPUT,
}
mock_gcs_output_event = kconfig._io.KlioWriteFileConfig.from_dict(
    mock_gcs_event_config
)
mock_bq_output_event_config = {
    "type": "bq",
    "project": "sigint",
    "dataset": "test-data",
    "table": "test-table",
    "schema": {"fields": [{"name": "n", "type": "t", "mode": "m"}]},
    "io_type": kconfig._io.KlioIOType.EVENT,
    "io_direction": kconfig._io.KlioIODirection.OUTPUT,
}

mock_bq_output_event = kconfig._io.KlioBigQueryEventOutput.from_dict(
    mock_bq_output_event_config
)


@pytest.mark.parametrize(
    "mock_event_output", ((mock_gcs_output_event), (mock_bq_output_event),)
)
def test_unverified_event_outputs(
    mocker,
    caplog,
    klio_config,
    mock_event_output,
    mock_storage,
    mock_publisher,
):
    mock_verify_gcs_bucket = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )
    mock_verify_pub_topic = mocker.patch.object(
        verify.VerifyJob, "_verify_pub_topic"
    )
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage

    job.klio_config.job_config.events.outputs = [mock_event_output]

    data_config = {
        "type": "gcs",
        "location": "test",
        "io_type": kconfig._io.KlioIOType.DATA,
        "io_direction": kconfig._io.KlioIODirection.OUTPUT,
    }
    job.klio_config.job_config.data.outputs = [
        kconfig._io.KlioGCSOutputDataConfig.from_dict(data_config)
    ]

    mock_verify_gcs_bucket.return_value = True

    actual = job._verify_outputs()

    assert actual
    mock_verify_pub_topic.assert_not_called()
    mock_verify_gcs_bucket.assert_called_with("test")
    assert 3 == len(caplog.records)


@pytest.mark.parametrize(
    "unverified_gcs, unverified_topic",
    ((False, False), (True, False), (True, True), (False, True)),
)
def test_verify_outputs(
    mocker,
    klio_config,
    unverified_gcs,
    unverified_topic,
    mock_storage,
    mock_publisher,
):
    mock_verify_gcs_bucket = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )
    mock_verify_pub_topic = mocker.patch.object(
        verify.VerifyJob, "_verify_pub_topic"
    )
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage

    event_config = {
        "type": "pubsub",
        "topic": "test",
        "io_type": kconfig._io.KlioIOType.EVENT,
        "io_direction": kconfig._io.KlioIODirection.OUTPUT,
    }
    job.klio_config.job_config.events.outputs = [
        kconfig._io.KlioPubSubEventOutput.from_dict(event_config)
    ]

    data_config = {
        "type": "gcs",
        "location": "test",
        "io_type": kconfig._io.KlioIOType.DATA,
        "io_direction": kconfig._io.KlioIODirection.OUTPUT,
    }
    job.klio_config.job_config.data.outputs = [
        kconfig._io.KlioGCSOutputDataConfig.from_dict(data_config)
    ]

    if unverified_gcs and unverified_topic:
        mock_verify_gcs_bucket.return_value = False
        mock_verify_pub_topic.return_value = False
        actual = job._verify_outputs()
        expected = False
        assert expected == actual
    elif unverified_topic:
        mock_verify_gcs_bucket.return_value = True
        mock_verify_pub_topic.return_value = False
        actual = job._verify_outputs()
        expected = False
        assert expected == actual
    elif unverified_gcs:
        mock_verify_gcs_bucket.return_value = False
        mock_verify_pub_topic.return_value = True
        actual = job._verify_outputs()
        expected = False
        assert expected == actual
    else:
        mock_verify_gcs_bucket.return_value = True
        mock_verify_pub_topic.return_value = True
        actual = job._verify_outputs()
        expected = True
        assert expected == actual

    mock_verify_gcs_bucket.assert_called_with("test")
    mock_verify_pub_topic.assert_called_with("test", "output")


@pytest.mark.parametrize(
    "event_topic, data_location, expected_log_count",
    (
        ("test", "test", 3),
        (None, "test", 4),
        ("test", None, 4),
        (None, None, 5),
    ),
)
def test_verify_outputs_logs(
    event_topic,
    data_location,
    expected_log_count,
    mocker,
    klio_config,
    mock_storage,
    mock_publisher,
    caplog,
):
    mock_verify_gcs = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )
    mock_verify_pub = mocker.patch.object(
        verify.VerifyJob, "_verify_pub_topic"
    )

    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage

    event_config = {
        "type": "pubsub",
        "topic": event_topic,
        "io_type": kconfig._io.KlioIOType.EVENT,
        "io_direction": kconfig._io.KlioIODirection.OUTPUT,
    }
    job.klio_config.job_config.events.outputs = [
        kconfig._io.KlioPubSubEventOutput.from_dict(event_config)
    ]

    data_config = {
        "type": "gcs",
        "location": data_location,
        "io_type": kconfig._io.KlioIOType.DATA,
        "io_direction": kconfig._io.KlioIODirection.OUTPUT,
    }
    job.klio_config.job_config.data.outputs = [
        kconfig._io.KlioGCSOutputDataConfig.from_dict(data_config)
    ]

    job._verify_outputs()

    assert expected_log_count == len(caplog.records)

    if data_location is not None:
        mock_verify_gcs.assert_called_with(data_location)

    if event_topic is not None:
        mock_verify_pub.assert_called_with(event_topic, "output")


@pytest.mark.parametrize(
    "unverified_staging, unverified_temp",
    ((False, False), (True, True), (True, False), (False, True)),
)
def test_verify_tmp_files(
    mocker, mock_storage, klio_config, unverified_staging, unverified_temp
):
    job = verify.VerifyJob(klio_config, True)
    job._storage_client = mock_storage
    job.klio_config.pipeline_options.staging_location = "test"
    job.klio_config.pipeline_options.temp_location = "test2"

    mock_verify_gcs = mocker.patch.object(
        verify.VerifyJob, "_verify_gcs_bucket"
    )

    if unverified_staging and unverified_temp:
        mock_verify_gcs.side_effect = [False, False]
        actual = job._verify_tmp_files()
        assert actual is False
    elif unverified_staging:
        mock_verify_gcs.side_effect = [False, True]
        actual = job._verify_tmp_files()
        assert actual is False
    elif unverified_temp:
        mock_verify_gcs.side_effect = [True, False]
        actual = job._verify_tmp_files()
        assert actual is False

    else:
        mock_verify_gcs.side_effect = [True, True]
        actual = job._verify_tmp_files()
        assert actual is True

    mock_verify_gcs.assert_any_call("test")
    mock_verify_gcs.assert_any_call("test2")
    assert 2 == mock_verify_gcs.call_count


def test_verify(
    mocker, klio_config, mock_storage, mock_publisher, mock_sub, caplog
):
    caplog.set_level(logging.INFO)
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage
    job._subscriber_client = mock_sub
    mock_verify_inputs = mocker.patch.object(job, "_verify_inputs")
    mock_verify_outputs = mocker.patch.object(job, "_verify_outputs")
    mock_verify_tmp = mocker.patch.object(job, "_verify_tmp_files")
    mock_verify_iam_roles = mocker.patch.object(job, "_verify_iam_roles")
    mock_verify_dashboard = mocker.patch.object(
        job, "_verify_stackdriver_dashboard"
    )
    mocker.patch.object(verify, "discovery")

    mock_verify_config = mocker.patch.object(kconfig, "KlioConfig")
    mock_verify_config.return_value = klio_config

    mock_storage.return_value = mock_storage
    mock_publisher.return_value = mock_publisher
    mock_sub.return_value = mock_sub

    mock_verify_inputs.return_value = True
    mock_verify_outputs.return_value = True
    mock_verify_tmp.return_value = True
    mock_verify_iam_roles.return_value = True

    job.verify_job()

    mock_verify_inputs.assert_called_once_with()
    mock_verify_outputs.assert_called_once_with()
    mock_verify_tmp.assert_called_once_with()
    assert mock_verify_iam_roles.called
    mock_verify_dashboard.assert_called_once_with()

    assert 1 == len(caplog.records)


def test_verify_raises_exception_raises_system_exit(mocker, klio_config):

    job = verify.VerifyJob(klio_config, False)
    mock_verify_all = mocker.patch.object(job, "_verify_all")
    mock_verify_all.return_value = False
    mock_verify_all.side_effect = Exception

    with pytest.raises(SystemExit):
        job.verify_job()


def test_verify_raises_system_exit(
    mocker, klio_config, mock_storage, mock_publisher, mock_sub, caplog
):
    caplog.set_level(logging.INFO)
    job = verify.VerifyJob(klio_config, False)
    job._publisher_client = mock_publisher
    job._storage_client = mock_storage
    job._subscriber_client = mock_sub

    mock_verify_inputs = mocker.patch.object(job, "_verify_inputs")
    mock_verify_outputs = mocker.patch.object(job, "_verify_outputs")
    mock_verify_tmp = mocker.patch.object(job, "_verify_tmp_files")
    mock_verify_iam_roles = mocker.patch.object(job, "_verify_iam_roles")
    mock_verify_dashboard = mocker.patch.object(
        job, "_verify_stackdriver_dashboard"
    )
    mocker.patch.object(verify, "discovery")

    mock_verify_config = mocker.patch.object(kconfig, "KlioConfig")
    mock_verify_config.return_value = klio_config

    mock_storage.return_value = mock_storage
    mock_publisher.return_value = mock_publisher
    mock_sub.return_value = mock_sub

    mock_verify_inputs.return_value = True
    mock_verify_outputs.return_value = False
    mock_verify_iam_roles.return_value = True
    mock_verify_tmp.return_value = True

    with pytest.raises(SystemExit):
        job.verify_job()

    mock_verify_inputs.assert_called_once_with()
    mock_verify_outputs.assert_called_once_with()
    mock_verify_tmp.assert_called_once_with()
    mock_verify_dashboard.assert_called_once_with()

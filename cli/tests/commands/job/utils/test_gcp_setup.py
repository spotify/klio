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

import pytest

from klio_cli.commands.job.utils import gcp_setup


@pytest.fixture
def mock_publisher(mocker):
    return mocker.patch.object(gcp_setup.pubsub_v1, "PublisherClient")


@pytest.fixture
def mock_subscriber(mocker):
    return mocker.patch.object(gcp_setup.pubsub_v1, "SubscriberClient")


@pytest.fixture
def mock_storage(mocker):
    return mocker.patch.object(gcp_setup.storage, "Client")


@pytest.fixture
def mock_create_topic(mocker):
    return mocker.patch.object(gcp_setup, "_create_topic_if_missing")


@pytest.fixture
def mock_create_storage(mocker):
    return mocker.patch.object(gcp_setup, "_create_bucket_if_missing")


@pytest.fixture
def context():
    return {
        "job_name": "test-job",
        "pipeline_options": {
            "project": "test-project",
            "region": "test-region",
        },
        "job_options": {
            "inputs": [
                {"topic": "test-input-topic", "subscription": "a-subscription"}
            ],
            "outputs": [
                {
                    "topic": "test-output-topic",
                    "data_location": "gs://test-output-location",
                }
            ],
        },
    }


@pytest.mark.parametrize(
    "existing,created,encountered_exception",
    [
        (None, "test-dashboard-created", False),
        ("test-dashboard-existed", None, False),
        (None, None, "exception-thrown"),
    ],
)
def test_create_stackdriver_group_if_missing(
    existing, created, encountered_exception, mocker, caplog
):
    mock_get_stackdriver_group_url = mocker.patch.object(
        gcp_setup.sd_utils, "get_stackdriver_group_url"
    )

    if encountered_exception:
        mock_get_stackdriver_group_url.side_effect = Exception
    else:
        mock_get_stackdriver_group_url.return_value = existing

    mock_create_stackdriver_group = mocker.patch.object(
        gcp_setup.sd_utils, "create_stackdriver_group"
    )
    mock_create_stackdriver_group.return_value = created

    ret_url = gcp_setup._create_stackdriver_group_if_missing(
        "test-project", "test-job", "test-region"
    )

    exp_url = existing if existing else created
    assert exp_url == ret_url

    mock_get_stackdriver_group_url.assert_called_once_with(
        "test-project", "test-job", "test-region"
    )
    if existing:
        mock_create_stackdriver_group.assert_not_called()
        assert 1 == len(caplog.records)
    elif created:
        mock_create_stackdriver_group.assert_called_once_with(
            "test-project", "test-job", "test-region"
        )
        assert not len(caplog.records)
    else:
        assert 1 == len(caplog.records)


def test_create_topic_if_missing(mock_publisher, mocker, caplog):
    mock_publisher.create_topic = mocker.Mock(return_value=True)

    gcp_setup._create_topic_if_missing(
        "test-input-topic", mock_publisher, "test-project"
    )

    mock_publisher.create_topic.assert_called_once_with("test-input-topic")
    assert 1 == len(caplog.records)


def test_create_topic_if_missing_exists(mock_publisher, mocker, caplog):
    err = gcp_setup.gcp_exceptions.AlreadyExists("test")
    mock_publisher.create_topic = mocker.Mock(side_effect=err)

    gcp_setup._create_topic_if_missing(
        "test-input-topic", mock_publisher, "test-project"
    )

    mock_publisher.create_topic.assert_called_once_with("test-input-topic")
    assert 1 == len(caplog.records)


def test_create_topic_if_missing_raises(mock_publisher, mocker, caplog):
    err = Exception("test")
    mock_publisher.create_topic = mocker.Mock(side_effect=err)

    with pytest.raises(SystemExit):
        gcp_setup._create_topic_if_missing(
            "test-input-topic", mock_publisher, "test-project"
        )

    mock_publisher.create_topic.assert_called_once_with("test-input-topic")
    assert 2 == len(caplog.records)


def test_create_subscription_if_missing(mock_subscriber, mocker, caplog):
    mock_subscriber.create_subscription = mocker.Mock(return_value=True)

    gcp_setup._create_subscription_if_missing(
        mock_subscriber, "test-input-topic", "test-subscription"
    )

    mock_subscriber.create_subscription.assert_called_once_with(
        name="test-subscription", topic="test-input-topic"
    )
    assert 1 == len(caplog.records)


def test_create_subscription_if_missing_exists(
    mock_subscriber, mocker, caplog
):
    err = gcp_setup.gcp_exceptions.AlreadyExists("test")
    mock_subscriber.create_subscription = mocker.Mock(side_effect=err)

    gcp_setup._create_subscription_if_missing(
        mock_subscriber, "test-input-topic", "test-subscription"
    )

    mock_subscriber.create_subscription.assert_called_once_with(
        name="test-subscription", topic="test-input-topic"
    )
    assert 1 == len(caplog.records)


def test_create_subscription_if_missing_raises(
    mock_subscriber, mocker, caplog
):
    err = Exception("test")
    mock_subscriber.create_subscription = mocker.Mock(side_effect=err)

    with pytest.raises(SystemExit):
        gcp_setup._create_subscription_if_missing(
            mock_subscriber, "test-input-topic", "test-subscription"
        )

    mock_subscriber.create_subscription.assert_called_once_with(
        name="test-subscription", topic="test-input-topic"
    )
    assert 2 == len(caplog.records)


def test_create_bucket_if_missing(mock_storage, mocker, caplog):
    mock_storage.create_bucket = mocker.Mock(return_value=True)

    gcp_setup._create_bucket_if_missing(
        "test-output-location", mock_storage, "test-project"
    )

    mock_storage.create_bucket.assert_called_once_with("test-output-location")
    assert 1 == len(caplog.records)


def test_create_bucket_if_missing_exists(mock_storage, mocker, caplog):
    err = gcp_setup.gcp_exceptions.Conflict("test")
    mock_storage.create_bucket = mocker.Mock(side_effect=err)

    gcp_setup._create_bucket_if_missing(
        "test-output-location", mock_storage, "test-project"
    )

    mock_storage.create_bucket.assert_called_once_with("test-output-location")
    assert 1 == len(caplog.records)


def test_create_bucket_if_missing_raises(mock_storage, mocker, caplog):
    err = Exception("test")
    mock_storage.create_bucket = mocker.Mock(side_effect=err)

    with pytest.raises(SystemExit):
        gcp_setup._create_bucket_if_missing(
            "test-output-location", mock_storage, "test-project"
        )
    mock_storage.create_bucket.assert_called_once_with("test-output-location")
    assert 2 == len(caplog.records)


def test_create_topics(
    context,
    mock_publisher,
    mock_subscriber,
    mock_storage,
    mock_create_topic,
    mock_create_storage,
    caplog,
):
    gcp_setup.create_topics(context)

    assert 1 == mock_publisher.call_count
    assert 1 == mock_subscriber.call_count
    assert 2 == mock_create_topic.call_count
    assert 3 == len(caplog.records)


def test_create_buckets(
    context, mock_storage, mock_create_storage, caplog,
):
    gcp_setup.create_buckets(context)

    assert 1 == mock_storage.call_count
    assert 1 == mock_create_storage.call_count
    assert 1 == len(caplog.records)


def test_create_buckets_unsupported(
    context, mock_storage, caplog,
):
    loc = "bq://not-a-gcs-location"
    context["job_options"]["outputs"][0]["data_location"] = loc
    gcp_setup.create_buckets(context)

    assert 1 == mock_storage.call_count
    assert 4 == len(caplog.records)


def test_create_stackdriver_dashboard(context, mocker, caplog):
    mock_create_stackdriver_if_missing = mocker.patch.object(
        gcp_setup, "_create_stackdriver_group_if_missing"
    )

    gcp_setup.create_stackdriver_dashboard(context)

    mock_create_stackdriver_if_missing.assert_called_once_with(
        "test-project", "test-job", "test-region"
    )

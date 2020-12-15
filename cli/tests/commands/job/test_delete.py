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

from klio_core import config as kconfig

from klio_cli.commands.job import delete as delete_job


@pytest.fixture
def config_data():
    return {
        "job_name": "test-job",
        "version": 1,
        "pipeline_options": {"project": "foo"},
        "job_config": {
            "events": {
                "inputs": [
                    {
                        "type": "pubsub",
                        "topic": "foo-topic",
                        "subscription": "foo-sub",
                    }
                ],
                "outputs": [{"type": "pubsub", "topic": "foo-topic-output"}],
            },
            "data": {
                "inputs": [{"type": "gcs", "location": "foo-input-location"}],
                "outputs": [
                    {"type": "gcs", "location": "foo-output-location"}
                ],
            },
        },
    }


@pytest.fixture
def klio_config(config_data):
    return kconfig.KlioConfig(config_data)


@pytest.fixture
def delete_job_inst(klio_config):
    return delete_job.DeleteJob(klio_config)


@pytest.fixture
def mock_confirm(mocker):
    return mocker.patch.object(delete_job.click, "confirm")


@pytest.fixture
def mock_prompt(mocker):
    return mocker.patch.object(delete_job.click, "prompt")


@pytest.fixture
def mock_publisher(mocker):
    return mocker.patch.object(
        delete_job.pubsub_v1, "PublisherClient", autospec=True
    )


@pytest.fixture
def mock_subscriber(mocker):
    return mocker.patch.object(
        delete_job.pubsub_v1, "SubscriberClient", autospec=True
    )


@pytest.fixture
def mock_storage(mocker):
    return mocker.patch.object(delete_job.storage, "Client", autospec=True)


@pytest.mark.parametrize(
    "confirmation,prompt,name,expected,effect",
    [
        ([False], [None], {}, False, None),
        ([False], ["foo"], "foo", False, None),
        ([True], ["foo"], "foo", True, None),
        ([True], ["pancake"], "milkshake", None, ValueError),
    ],
)
def test_confirmation_dialog(
    confirmation,
    prompt,
    name,
    expected,
    effect,
    delete_job_inst,
    mock_confirm,
    mock_prompt,
):
    mock_confirm.side_effect = confirmation
    mock_prompt.side_effect = prompt
    if effect:
        with pytest.raises(ValueError):
            delete_job_inst._confirmation_dialog("meh", name)
    else:
        assert expected == delete_job_inst._confirmation_dialog("meh", name)


@pytest.mark.parametrize(
    "confirm,expected",
    [
        (
            # do not confirm to delete an resource
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [False, False, False, False, False, False],
            {
                "topic": [],
                "subscription": [],
                "location": [],
                "stackdriver_group": False,
            },
        ),
        (
            # only confirm stackdriver
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [False, False, False, False, False, True],
            {
                "topic": [],
                "subscription": [],
                "location": [],
                "stackdriver_group": True,
            },
        ),
        (
            # gcs buckets & stackdriver
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [False, False, False, True, True, True],
            {
                "topic": [],
                "subscription": [],
                "location": ["foo-input-location", "foo-output-location"],
                "stackdriver_group": True,
            },
        ),
        (
            # output topic, gcs buckets & stackdriver
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [False, False, True, True, True, True],
            {
                "topic": ["foo-topic-output"],
                "subscription": [],
                "location": ["foo-input-location", "foo-output-location"],
                "stackdriver_group": True,
            },
        ),
        (
            # subscription, output topic, gcs buckets & stackdriver
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [False, True, True, True, True, True],
            {
                "topic": ["foo-topic-output"],
                "subscription": ["foo-sub"],
                "location": ["foo-input-location", "foo-output-location"],
                "stackdriver_group": True,
            },
        ),
        (
            # everything!
            # (2 topics, 1 sub, 2 gcs locations, 1 stackdriver)
            [True, True, True, True, True, True],
            {
                "topic": ["foo-topic", "foo-topic-output"],
                "subscription": ["foo-sub"],
                "location": ["foo-input-location", "foo-output-location"],
                "stackdriver_group": True,
                "stackdriver_group": True,
            },
        ),
    ],
)
def test_get_resources(confirm, expected, delete_job_inst, mocker):
    mock_sd = mocker.patch.object(delete_job.sd_utils, "generate_group_meta")
    mock_sd.return_value = (None, "fake-stackdriver-group")

    mock_confirmation = mocker.patch.object(
        delete_job_inst, "_confirmation_dialog"
    )
    mock_confirmation.side_effect = confirm

    resources = delete_job_inst._get_resources()
    assert resources == expected


@pytest.mark.parametrize("effect,record_count", [(Exception, 6), (None, 3)])
def test_delete_subscription(
    effect, record_count, mocker, mock_subscriber, caplog, delete_job_inst
):
    mock_subscriber.return_value.delete_subscription.side_effect = effect
    delete_job_inst._delete_subscriptions(["a", "b", "c"])
    mock_subscriber.return_value.delete_subscription.assert_has_calls(
        [
            mocker.call(request={"subscription": "a"}),
            mocker.call(request={"subscription": "b"}),
            mocker.call(request={"subscription": "c"}),
        ]
    )
    assert 3 == mock_subscriber.return_value.delete_subscription.call_count
    assert record_count == len(caplog.records)


@pytest.mark.parametrize("effect,record_count", [(Exception, 6), (None, 3)])
def test_delete_topic(
    effect, record_count, mocker, mock_publisher, caplog, delete_job_inst
):
    mock_publisher.return_value.delete_topic.side_effect = effect
    delete_job_inst._delete_topics(["a", "b", "c"])
    mock_publisher.return_value.delete_topic.assert_has_calls(
        [
            mocker.call(request={"topic": "a"}),
            mocker.call(request={"topic": "b"}),
            mocker.call(request={"topic": "c"}),
        ]
    )
    assert 3 == mock_publisher.return_value.delete_topic.call_count
    assert record_count == len(caplog.records)


@pytest.mark.parametrize("effect,record_count", [(Exception, 6), (None, 4)])
def test_delete_buckets(
    effect, record_count, mocker, mock_storage, caplog, delete_job_inst
):
    mock_storage.return_value.get_bucket.return_value.delete.side_effect = (
        effect
    )
    delete_job_inst._delete_buckets(
        "test-project", ["gs://a", "gs://b/d", "c"]
    )
    mock_storage.assert_called_once_with("test-project")
    mock_storage.return_value.get_bucket.assert_has_calls(
        [mocker.call("a"), mocker.call("b")], any_order=True
    )
    assert 2 == mock_storage.return_value.get_bucket.call_count

    mock_storage.return_value.get_bucket.return_value.delete.assert_has_calls(
        [mocker.call(force=True), mocker.call(force=True)]
    )
    assert (
        2
        == mock_storage.return_value.get_bucket.return_value.delete.call_count
    )
    assert record_count == len(caplog.records)


@pytest.mark.parametrize("sd_group", [True, False])
def test_delete(sd_group, mocker, mock_confirm, mock_prompt, klio_config):
    mock_get_resources = mocker.patch.object(
        delete_job.DeleteJob, "_get_resources"
    )
    mock_get_resources.return_value = {
        "topic": ["a", "d"],
        "subscription": ["b", "e"],
        "location": ["c", "f"],
        "stackdriver_group": sd_group,
    }
    mock_delete_subscriptions = mocker.patch.object(
        delete_job.DeleteJob, "_delete_subscriptions"
    )
    mock_delete_topics = mocker.patch.object(
        delete_job.DeleteJob, "_delete_topics"
    )
    mock_delete_buckets = mocker.patch.object(
        delete_job.DeleteJob, "_delete_buckets"
    )
    mock_delete_stackdriver_group = mocker.patch.object(
        delete_job.sd_utils, "delete_stackdriver_group"
    )

    delete_job.DeleteJob(klio_config).delete()

    mock_delete_subscriptions.assert_called_once_with(["b", "e"])
    mock_delete_topics.assert_called_once_with(["a", "d"])
    mock_delete_buckets.assert_called_once_with("foo", ["c", "f"])
    if sd_group:
        mock_delete_stackdriver_group.assert_called_once_with(
            "foo", "test-job", "europe-west1"
        )
    else:
        mock_delete_stackdriver_group.assert_not_called()

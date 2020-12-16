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

import collections

import pytest

from klio_cli.utils import stackdriver_utils as sd_utils


stackdriver_group = collections.namedtuple(
    "StackdriverGroup", ["name", "display_name"]
)


@pytest.fixture
def sd_client(mocker, monkeypatch):
    client = mocker.Mock()
    monkeypatch.setattr(sd_utils.monitoring, "GroupServiceClient", client)
    return client.return_value


@pytest.fixture
def groups():
    base_display_name = "test-job-name-{}-test-region-klio-dashboard"
    base_name = "projects/test-gcp-project/groups/123456789{}"
    ret_groups = []
    for i in range(3):
        ret_groups.append(
            stackdriver_group(
                name=base_name.format(i),
                display_name=base_display_name.format(i),
            )
        )
    return ret_groups


def test_generate_group_meta():
    assert sd_utils.generate_group_meta("a", "b", "c") == (
        "projects/a",
        "b-c-klio-dashboard",
    )


@pytest.mark.parametrize(
    "job_name,exp_ret", [("test-job-name-1", True), ("test-job-name-4", False)]
)
def test_get_stackdriver_group_url(sd_client, groups, job_name, exp_ret):
    sd_client.list_groups.return_value = groups

    ret_url = sd_utils.get_stackdriver_group_url(
        "test-gcp-project", job_name, "test-region"
    )

    exp_url = None
    if exp_ret:
        exp_url = (
            "https://app.google.stackdriver.com/groups/1234567891/"
            "test-job-name-1-test-region-klio-dashboard?"
            "project=test-gcp-project"
        )

    assert exp_url == ret_url
    sd_client.list_groups.assert_called_once_with(
        request={"name": "projects/test-gcp-project"}
    )


def test_get_stackdriver_group_url_raises(sd_client):
    sd_client.list_groups.side_effect = Exception("fuuuuu")

    with pytest.raises(Exception):
        sd_utils.get_stackdriver_group_url(
            "test-gcp-project", "test-job-name-1", "test-region"
        )


def test_create_stackdriver_group(sd_client, groups, caplog):
    sd_client.create_group.return_value = groups[0]

    ret_url = sd_utils.create_stackdriver_group(
        "test-gcp-project", "test-job-name-0", "test-region"
    )

    exp_url = (
        "https://app.google.stackdriver.com/groups/1234567890/"
        "test-job-name-0-test-region-klio-dashboard?"
        "project=test-gcp-project"
    )
    assert exp_url == ret_url

    group_arg = {
        "display_name": "test-job-name-0-test-region-klio-dashboard",
        "filter": "resource.metadata.name=starts_with(test-job-name-0)",
    }
    sd_client.create_group.assert_called_once_with(
        request={"name": "projects/test-gcp-project", "group": group_arg}
    )
    assert 1 == len(caplog.records)


def test_create_stackdriver_group_errors(sd_client, caplog):
    sd_client.create_group.side_effect = Exception("fuuuu")

    ret_url = sd_utils.create_stackdriver_group(
        "test-gcp-project", "test-job-name-0", "test-region"
    )

    assert not ret_url
    assert 1 == len(caplog.records)


@pytest.mark.parametrize(
    "job_name,side_effect,exp_log_level",
    (
        ("test-job-name-1", None, "INFO"),
        ("test-job-name-1", Exception("meow"), "ERROR"),
        ("kittehs", None, "WARNING"),
    ),
)
def test_delete_stackdriver_group(
    job_name, side_effect, exp_log_level, groups, sd_client, caplog
):
    sd_client.list_groups.return_value = groups
    sd_client.list_groups.side_effect = side_effect

    sd_utils.delete_stackdriver_group(
        "test-gcp-project", job_name, "test-region"
    )

    assert 1 == len(caplog.records)
    assert exp_log_level == caplog.records[0].levelname

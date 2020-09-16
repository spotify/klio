# Copyright 2020 Spotify AB
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

from klio_cli.commands.job import audit as audit_job


@pytest.fixture
def mock_os_environ(mocker):
    return mocker.patch.dict(
        audit_job.base.os.environ, {"USER": "cookiemonster"}
    )


@pytest.fixture
def audit_pipeline(mock_os_environ):
    return audit_job.AuditPipeline("job/dir", "klio_config", "docker_config")


def test_get_environment(audit_pipeline):
    gcreds = "/usr/gcloud/application_default_credentials.json"
    exp_envs = {
        "PYTHONPATH": "/usr/src/app",
        "GOOGLE_APPLICATION_CREDENTIALS": gcreds,
        "USER": "cookiemonster",
        "KLIO_TEST_MODE": "true",
    }

    assert exp_envs == audit_pipeline._get_environment()


@pytest.mark.parametrize(
    "list_steps,exp_cmd", ((True, ["audit", "--list"]), (False, ["audit"]))
)
def test_get_command(list_steps, exp_cmd, audit_pipeline):
    assert exp_cmd == audit_pipeline._get_command(list_steps)

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

from klio_exec.commands.audit_steps import base


@pytest.fixture
def klio_config(mocker):
    conf = mocker.Mock()
    conf.pipeline_options = mocker.Mock()
    conf.pipeline_options.experiments = []
    return conf


@pytest.fixture
def mock_emit_warning(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(base.BaseKlioAuditStep, "emit_warning", mock)
    return mock


@pytest.fixture
def mock_emit_error(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(base.BaseKlioAuditStep, "emit_error", mock)
    return mock

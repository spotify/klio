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

from klio_core import config as kconfig
from klio_core import utils as core_utils


def assert_execution_success(result):
    """Helper for testing CLI commands that emits errors if execution failed"""
    if result.exception:
        if result.stdout:
            print("captured stdout: {}".format(result.stdout))
        raise result.exception

    assert 0 == result.exit_code


class MockKlioConfig(object):
    def __init__(self, module, mocker, monkeypatch, patch_os_getcwd):

        self.mock_get_config = mocker.patch.object(
            core_utils, "get_config_by_path"
        )
        self.mock_get_config_job_dir = mocker.Mock()
        monkeypatch.setattr(
            module.core_utils,
            "get_config_job_dir",
            self.mock_get_config_job_dir,
        )

        self.module = module
        self.patch_os_getcwd = patch_os_getcwd
        self.mocker = mocker
        self.monkeypatch = monkeypatch

    def setup(
        self,
        config_data,
        config_file,
        config_override=None,
        job_dir_override=None,
    ):
        self.config_override = config_override
        self.job_dir_override = job_dir_override
        self.config_data = config_data
        self.config_file = config_file

        self.effective_job_dir = self.job_dir_override or self.patch_os_getcwd
        self.effective_config_path = config_override or config_file

        self.mock_warn_if_py2_job = self.mocker.Mock()
        self.monkeypatch.setattr(
            self.module.core_utils,
            "warn_if_py2_job",
            self.mock_warn_if_py2_job,
        )

        self.mock_get_config_job_dir.return_value = (
            self.effective_job_dir,
            self.effective_config_path,
        )

        self.mock_get_config.return_value = config_data

        self.meta = core_utils.KlioConfigMeta(
            job_dir=self.effective_job_dir,
            config_file=config_override,
            config_path=self.effective_config_path,
        )

        self.klio_config = kconfig.KlioConfig(config_data)

        self.mock_klio_config = self.mocker.patch.object(
            core_utils.config, "KlioConfig"
        )
        self.mock_klio_config.return_value = self.klio_config

        return self.klio_config

    def assert_calls(self):
        self.mock_get_config_job_dir.assert_called_once_with(
            self.job_dir_override, self.config_override
        )
        self.mock_get_config.assert_called_once_with(
            self.effective_config_path
        )
        self.mock_klio_config.assert_called_once_with(
            self.config_data, raw_overrides=(), raw_templates=()
        )
        self.mock_warn_if_py2_job.assert_called_once_with(
            self.effective_job_dir
        )

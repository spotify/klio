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

import pytest
import yaml

from klio_core import config

from klio.metrics import logger as logger_metrics
from klio.metrics import stackdriver as sd_metrics
from klio.transforms import core as core_transforms


@pytest.mark.parametrize(
    "runner,metrics_config,exp_clients,exp_logger_enabled",
    (
        # default metrics client config for respective runner
        ("dataflow", {}, (sd_metrics.StackdriverLogMetricsClient,), None),
        ("direct", {}, (logger_metrics.MetricsLoggerClient,), True),
        # explicitly turn on metrics for respective runner
        (
            "dataflow",
            {"stackdriver_logger": True},
            (sd_metrics.StackdriverLogMetricsClient,),
            None,
        ),
        (
            "direct",
            {"logger": True},
            (logger_metrics.MetricsLoggerClient,),
            True,
        ),
        # turn off default client for respective runner
        (
            "dataflow",
            {"stackdriver_logger": False},
            (logger_metrics.MetricsLoggerClient,),
            False,
        ),
        (
            "direct",
            {"logger": False},
            (logger_metrics.MetricsLoggerClient,),
            False,
        ),
        # ignore SD config when on direct
        (
            "direct",
            {"stackdriver_logger": True},
            (logger_metrics.MetricsLoggerClient,),
            True,
        ),
        # ignore logger config when on dataflow
        (
            "dataflow",
            {"logger": False},
            (sd_metrics.StackdriverLogMetricsClient,),
            None,
        ),
    ),
)
def test_klio_metrics(
    runner,
    metrics_config,
    exp_clients,
    exp_logger_enabled,
    klio_config,
    mocker,
    monkeypatch,
):
    klio_ns = core_transforms.KlioContext()
    # sanity check / clear out thread local
    klio_ns._thread_local.klio_metrics = None

    monkeypatch.setattr(klio_config.pipeline_options, "runner", runner, None)
    monkeypatch.setattr(klio_config.job_config, "metrics", metrics_config)
    mock_config = mocker.PropertyMock(return_value=klio_config)
    monkeypatch.setattr(core_transforms.KlioContext, "config", mock_config)

    registry = klio_ns.metrics

    for actual_relay in registry._relays:
        assert isinstance(actual_relay, exp_clients)
        if isinstance(actual_relay, logger_metrics.MetricsLoggerClient):
            assert exp_logger_enabled is not actual_relay.disabled


@pytest.mark.parametrize("usr_local_exists", (True, False))
@pytest.mark.parametrize("usr_glob_exists", (True, False))
def test_load_config_from_file(
    usr_local_exists, usr_glob_exists, config_dict, mocker, monkeypatch,
):
    monkeypatch.setattr(os.path, "exists", lambda x: usr_local_exists)
    effective_klio_yaml_file = "/usr/src/app/klio-job-run-effective.yaml"
    expected_open_file = "/usr/src/config/.effective-klio-job.yaml"

    if usr_local_exists:
        expected_open_file = "/usr/local/klio-job-run-effective.yaml"
    elif usr_glob_exists:
        expected_open_file = effective_klio_yaml_file

    mock_iglob = mocker.Mock()
    if usr_glob_exists:
        mock_iglob.return_value = [effective_klio_yaml_file]
    else:
        mock_iglob.return_value = []

    monkeypatch.setattr(core_transforms.glob, "iglob", mock_iglob)

    open_name = "klio.transforms.core.open"
    config_str = yaml.dump(config_dict)
    m_open = mocker.mock_open(read_data=config_str)
    m = mocker.patch(open_name, m_open)

    klio_config = core_transforms.RunConfig._load_config_from_file()

    m.assert_called_once_with(expected_open_file, "r")
    assert isinstance(klio_config, config.KlioConfig)
    if not usr_local_exists:
        mock_iglob.assert_called_once_with(
            "/usr/**/klio-job-run-effective.yaml", recursive=True
        )
    else:
        mock_iglob.assert_not_called()


def test_load_config_from_file_raises(config_dict, mocker, monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda x: False)

    mock_iglob = mocker.Mock()
    mock_iglob.return_value = iter([])
    monkeypatch.setattr(core_transforms.glob, "iglob", mock_iglob)

    with pytest.raises(IOError):
        core_transforms.RunConfig._load_config_from_file()


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_job_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms.KlioContext, "_create_klio_job_obj", mock_func
    )

    klio_ns = core_transforms.KlioContext()

    if not thread_local_ret:
        # sanity check / clear out thread local
        klio_ns._thread_local.klio_job = None
    else:
        klio_ns._thread_local.klio_job = mocker.Mock()

    ret_value = klio_ns.job

    if not thread_local_ret:
        mock_func.assert_called_once_with()
        assert (
            mock_func.return_value
            == ret_value
            == klio_ns._thread_local.klio_job
        )
    else:
        mock_func.assert_not_called()
        assert klio_ns._thread_local.klio_job == ret_value

    klio_ns._thread_local.klio_job = None


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_metrics_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms.KlioContext, "_get_metrics_registry", mock_func
    )

    klio_ns = core_transforms.KlioContext()

    if not thread_local_ret:
        # sanity check / clear out thread local
        klio_ns._thread_local.klio_metrics = None
    else:
        klio_ns._thread_local.klio_metrics = mocker.Mock()

    ret_value = klio_ns.metrics

    if not thread_local_ret:
        mock_func.assert_called_once_with()
        assert (
            mock_func.return_value
            == ret_value
            == klio_ns._thread_local.klio_metrics
        )
    else:
        mock_func.assert_not_called()
        assert klio_ns._thread_local.klio_metrics == ret_value

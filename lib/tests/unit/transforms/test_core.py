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

from klio.metrics import logger as logger_metrics
from klio.metrics import native as native_metrics
from klio.metrics import shumway
from klio.transforms import core as core_transforms


@pytest.mark.parametrize(
    "runner,metrics_config,exp_clients",
    (
        # default metrics client config for respective runner
        ("direct", {}, [logger_metrics.MetricsLoggerClient]),
        ("directgkerunner", {}, [shumway.ShumwayMetricsClient]),
        ("dataflow", {}, []),  # default to native
        # explicitly turn on metrics for respective runner
        ("direct", {"logger": True}, [logger_metrics.MetricsLoggerClient],),
        ("directgkerunner", {"shumway": True}, [shumway.ShumwayMetricsClient]),
        # turn off default client for respective runner
        ("direct", {"logger": False}, []),
        ("directgkerunner", {"shumway": False}, []),
        # turn off other clients should have no effect
        ("directgkerunner", {"logger": False}, [shumway.ShumwayMetricsClient]),
        ("direct", {"shumway": False}, [logger_metrics.MetricsLoggerClient]),
        ("dataflow", {"shumway": False, "logger": False}, []),
    ),
)
def test_klio_metrics(
    runner, metrics_config, exp_clients, klio_config, mocker, monkeypatch,
):
    # all should have the native metrics client
    exp_clients.append(native_metrics.NativeMetricsClient)

    klio_ns = core_transforms.KlioContext()
    # sanity check / clear out thread local
    klio_ns._thread_local.klio_metrics = None

    monkeypatch.setattr(klio_config.pipeline_options, "runner", runner, None)
    monkeypatch.setattr(klio_config.job_config, "metrics", metrics_config)
    mock_config = mocker.PropertyMock(return_value=klio_config)
    monkeypatch.setattr(core_transforms.KlioContext, "config", mock_config)

    registry = klio_ns.metrics

    assert len(exp_clients) == len(registry._relays)
    for actual_relay in registry._relays:
        assert any([isinstance(actual_relay, ec) for ec in exp_clients])
        if isinstance(actual_relay, logger_metrics.MetricsLoggerClient):
            assert actual_relay.disabled is False


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


# FIXME: For some reason, this test's mock that is patched to `klio.transforms.
# core.KlioContext._get_metrics_registry` stays around after the test is done.
# This causes failures in test_helpers where counter objects are expected but
# what's actually returned is the _get_metrics_registry mock.
@pytest.mark.skip("FIXME: patches linger creating failures in other modules")
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

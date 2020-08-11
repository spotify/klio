# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import os

import apache_beam as beam
import pytest
import yaml

from klio_core import config
from klio_core import dataflow
from klio_core.proto import klio_pb2

from klio.metrics import logger as logger_metrics
from klio.metrics import stackdriver as sd_metrics
from klio.transforms import core as core_transforms


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, "fixtures")


class PlainBaseKlass(object):
    pass


class PlainKlass(PlainBaseKlass):
    def process(self):
        pass


@pytest.fixture
def mock_parse_klio_message(mocker):
    return mocker.patch.object(
        core_transforms.v1_msg_handler, "parse_klio_message"
    )


@pytest.fixture
def mock_is_original_process_func(mocker):
    mock = mocker.patch.object(
        core_transforms._utils, "is_original_process_func"
    )
    return mock


def test_create_thread_pool(mocker, monkeypatch, user_dofn, klio_config):
    dofn_inst = user_dofn()
    mock_config = mocker.Mock()
    monkeypatch.setattr(
        core_transforms._KlioNamespace, "_load_config_from_file", mock_config
    )
    monkeypatch.setattr(
        dofn_inst._klio.config.job_config, "thread_pool_processes", 5
    )
    mock_thread_pool = mocker.patch.object(core_transforms.pool, "ThreadPool")

    dofn_inst._klio._create_thread_pool()

    mock_thread_pool.assert_called_once_with(processes=5)


@pytest.mark.parametrize(
    "has_deps,given_input_topic,ret_input_topic,region,exp_log_count",
    (
        (True, None, "a-topic", None, 1),
        (True, None, "a-topic", "a-region", 1),
        (True, "a-topic", "a-topic", None, 1),
        (True, "a-topic", "a-topic", "a-region", 1),
        (True, None, Exception("foo"), None, 2),
        (True, None, None, None, 1),
        (False, None, None, None, 0),
    ),
)
def test_get_parent_jobs(
    has_deps,
    user_dofn,
    given_input_topic,
    ret_input_topic,
    region,
    exp_log_count,
    klio_config,
    mocker,
    monkeypatch,
    caplog,
):
    mock_dataflow = mocker.Mock()
    dofn_inst = user_dofn()
    mocker.patch.object(
        core_transforms._KlioNamespace, "_load_config_from_file"
    )
    mocker.patch.object(core_transforms._KlioNamespace, "config")
    dofn_inst._klio.config = klio_config
    if region:
        klio_config.job_config.dependencies[0]["region"] = region
    if given_input_topic:
        klio_config.job_config.dependencies[0][
            "input_topic"
        ] = given_input_topic

    if isinstance(ret_input_topic, Exception):
        mock_dataflow.get_job_input_topic.side_effect = ret_input_topic
    else:
        mock_dataflow.get_job_input_topic.return_value = ret_input_topic

    monkeypatch.setattr(dataflow, "get_dataflow_client", lambda: mock_dataflow)

    if not has_deps:
        klio_config.job_config.dependencies = []

    ret_parent_jobs = dofn_inst._klio._get_parent_jobs()

    parent_job_name = "test-parent-job"
    parent_gcp_project = "sigint"

    if has_deps and not given_input_topic:
        mock_dataflow.get_job_input_topic.assert_called_once_with(
            parent_job_name, parent_gcp_project, region
        )

    if ret_input_topic and not isinstance(ret_input_topic, Exception):
        exp_parent_job = klio_pb2.KlioJob()
        exp_parent_job.job_name = parent_job_name
        exp_parent_job.gcp_project = parent_gcp_project
        exp_job_input = exp_parent_job.JobInput()
        exp_job_input.topic = ret_input_topic
        exp_parent_job.inputs.extend([exp_job_input])
        expected = [exp_parent_job.SerializeToString()]
        assert expected == ret_parent_jobs
    else:
        assert [] == ret_parent_jobs
    assert exp_log_count == len(caplog.records)


def test_klio_dofn_metaclass(
    mock_is_original_process_func, mock_parse_klio_message
):
    mock_is_original_process_func.return_value = True
    clsdict = {"process": PlainKlass.process}

    new_plain_klass = core_transforms.KlioDoFnMetaclass(
        "PlainKlass", (PlainBaseKlass,), clsdict
    )

    mock_is_original_process_func.assert_called_once_with(
        clsdict, (PlainBaseKlass,), base_class="KlioBaseDoFn"
    )

    actual_process_method = getattr(new_plain_klass, "process")
    assert mock_parse_klio_message.return_value == actual_process_method
    assert hasattr(new_plain_klass, "_klio")
    assert hasattr(new_plain_klass._klio, "logger")


def test_klio_dofn_metaclass_no_process(
    mock_is_original_process_func, mock_parse_klio_message
):
    clsdict = {"_klio_abstract_methods": [], "_klio_all_methods": []}
    mock_is_original_process_func.return_value = False
    new_plain_klass = core_transforms.KlioDoFnMetaclass(
        "PlainKlass", (PlainBaseKlass,), clsdict
    )

    mock_is_original_process_func.assert_called_once_with(
        clsdict, (PlainBaseKlass,), base_class="KlioBaseDoFn"
    )

    actual_process_method = getattr(new_plain_klass, "process", None)
    assert not actual_process_method


def test_klio_dofn_metaclass_test_mode(
    mocker, monkeypatch, mock_is_original_process_func
):
    monkeypatch.setenv("KLIO_TEST_MODE", "TRUE")

    mock_has_abstract_methods_impl = mocker.patch.object(
        core_transforms._utils, "has_abstract_methods_implemented"
    )

    clsdict = {"_klio_abstract_methods": [], "_klio_all_methods": []}

    new_plain_klass = core_transforms.KlioDoFnMetaclass(
        "PlainKlass", (PlainBaseKlass,), clsdict
    )

    mock_is_original_process_func.assert_not_called()
    mock_has_abstract_methods_impl.assert_called_once_with(
        new_plain_klass, "PlainKlass", (PlainBaseKlass,)
    )

    actual_process_method = getattr(new_plain_klass, "process", None)
    assert not actual_process_method

    assert hasattr(new_plain_klass, "_klio")


def test_klio_base_dofn(user_dofn, klio_config, mocker):

    assert issubclass(user_dofn, core_transforms.KlioBaseDoFn)
    assert issubclass(user_dofn, beam.DoFn)
    input_data_exists_method = getattr(user_dofn, "input_data_exists", None)
    assert input_data_exists_method
    assert callable(input_data_exists_method)

    output_data_exists_method = getattr(user_dofn, "output_data_exists", None)
    assert output_data_exists_method
    assert callable(output_data_exists_method)

    dofn_inst = user_dofn()
    mocker.patch.object(core_transforms._KlioNamespace, "config")
    dofn_inst._klio.config = klio_config

    assert hasattr(dofn_inst, "_klio")
    assert isinstance(dofn_inst._klio, core_transforms._KlioNamespace)
    assert hasattr(dofn_inst._klio, "job")
    assert hasattr(dofn_inst._klio, "config")
    assert hasattr(dofn_inst._klio, "_transform_name")
    assert dofn_inst.__class__.__name__ == dofn_inst._klio._transform_name


def test_klio_dofn_no_input_data_exists():
    with pytest.raises(NotImplementedError):

        class NoInputData(core_transforms.KlioBaseDoFn):
            def process(self, element):
                pass


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
    klio_ns = core_transforms._KlioNamespace()
    # sanity check / clear out thread local
    klio_ns._thread_local.klio_metrics = None

    monkeypatch.setattr(klio_config.pipeline_options, "runner", runner, None)
    monkeypatch.setattr(klio_config.job_config, "metrics", metrics_config)
    mock_config = mocker.PropertyMock(return_value=klio_config)
    monkeypatch.setattr(core_transforms._KlioNamespace, "config", mock_config)

    registry = klio_ns.metrics

    for actual_relay in registry._relays:
        assert isinstance(actual_relay, exp_clients)
        if isinstance(actual_relay, logger_metrics.MetricsLoggerClient):
            assert exp_logger_enabled is not actual_relay.disabled


@pytest.mark.parametrize("exists", (True, False))
def test_load_config_from_file(exists, config_dict, mocker, monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda x: exists)

    klio_yaml_file = "/usr/src/config/.effective-klio-job.yaml"
    if not exists:
        klio_yaml_file = "/usr/lib/python/site-packages/klio/klio-job.yaml"
        mock_iglob = mocker.Mock()
        mock_iglob.return_value = iter([klio_yaml_file])
        monkeypatch.setattr(core_transforms.glob, "iglob", mock_iglob)

    open_name = "klio.transforms.core.open"
    config_str = yaml.dump(config_dict)
    m_open = mocker.mock_open(read_data=config_str)
    m = mocker.patch(open_name, m_open)

    klio_ns = core_transforms._KlioNamespace()

    klio_config = klio_ns._load_config_from_file()

    m.assert_called_once_with(klio_yaml_file, "r")
    assert isinstance(klio_config, config.KlioConfig)
    if not exists:
        mock_iglob.assert_called_once_with(
            "/usr/**/klio-job.yaml", recursive=True
        )


def test_load_config_from_file_raises(config_dict, mocker, monkeypatch):
    monkeypatch.setattr(os.path, "exists", lambda x: False)

    mock_iglob = mocker.Mock()
    mock_iglob.return_value = iter([])
    monkeypatch.setattr(core_transforms.glob, "iglob", mock_iglob)

    klio_ns = core_transforms._KlioNamespace()

    with pytest.raises(IOError):
        klio_ns._load_config_from_file()


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_parent_jobs_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms._KlioNamespace, "_get_parent_jobs", mock_func
    )

    klio_ns = core_transforms._KlioNamespace()

    if not thread_local_ret:
        # sanity check / clear out thread local
        klio_ns._thread_local.parent_jobs = None
    else:
        klio_ns._thread_local.parent_jobs = mocker.Mock()

    ret_value = klio_ns.parent_jobs

    if not thread_local_ret:
        mock_func.assert_called_once_with()
        assert (
            mock_func.return_value
            == ret_value
            == klio_ns._thread_local.parent_jobs
        )
    else:
        mock_func.assert_not_called()
        assert klio_ns._thread_local.parent_jobs == ret_value


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_threadpool_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms._KlioNamespace, "_create_thread_pool", mock_func
    )

    klio_ns = core_transforms._KlioNamespace()

    if not thread_local_ret:
        # sanity check / clear out thread local
        klio_ns._thread_local.klio_thread_pool = None
    else:
        klio_ns._thread_local.klio_thread_pool = mocker.Mock()

    ret_value = klio_ns._thread_pool

    if not thread_local_ret:
        mock_func.assert_called_once_with()
        assert (
            mock_func.return_value
            == ret_value
            == klio_ns._thread_local.klio_thread_pool
        )
    else:
        mock_func.assert_not_called()
        assert klio_ns._thread_local.klio_thread_pool == ret_value


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_job_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms._KlioNamespace, "_create_klio_job_obj", mock_func
    )

    klio_ns = core_transforms._KlioNamespace()

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


@pytest.mark.parametrize("thread_local_ret", (True, False))
def test_metrics_property(thread_local_ret, mocker, monkeypatch):
    mock_func = mocker.Mock()
    monkeypatch.setattr(
        core_transforms._KlioNamespace, "_get_metrics_registry", mock_func
    )

    klio_ns = core_transforms._KlioNamespace()

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

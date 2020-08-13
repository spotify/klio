# Copyright 2020 Spotify AB

import logging

import pytest

from klio_core.proto import klio_pb2

import run


@pytest.fixture
def caplog(caplog):
    """Set global test logging levels."""
    caplog.set_level(logging.DEBUG)
    return caplog


@pytest.fixture
def klio_msg():
    element = b"s0m3_tr4ck_1d"
    msg = klio_pb2.KlioMessage()
    msg.data.v2.element = element
    return msg

@pytest.mark.parametrize(
    "func_to_test,exp_log_prefix",
    (
        (run.first_func, "[first_func]:"),
        (run.second_func, "[second_func]:"),
        (run.combined_func, "[combined_func]:"),
    )
)
def test_process_funcs(func_to_test, exp_log_prefix, klio_msg, caplog):
    ret = func_to_test(klio_msg.SerializeToString())

    assert klio_msg.SerializeToString() == ret
    assert 1 == len(caplog.records)
    exp_log_msg = f"{exp_log_prefix} {klio_msg.data.v2.element}"
    assert exp_log_msg == caplog.records[0].message
    assert "INFO" == caplog.records[0].levelname

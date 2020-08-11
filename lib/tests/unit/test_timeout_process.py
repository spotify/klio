# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

from __future__ import absolute_import

import multiprocessing

import pytest

from klio import timeout_process


@pytest.fixture
def args():
    return "whatup", "nuthin"


@pytest.fixture
def mock_process_generator(mocker):
    mock = mocker.Mock()

    def process(*args, **kwargs):
        mock(*args, **kwargs)
        yield "done"

    return mock, process


@pytest.fixture
def mock_process(mocker):
    mock = mocker.Mock()

    def process(*args, **kwargs):
        mock(*args, **kwargs)
        return "done"

    return mock, process


@pytest.fixture
def mock_process_raises(mocker):
    mock = mocker.Mock()

    def process(*args, **kwargs):
        mock(*args, **kwargs)
        raise Exception("Whatup, I'm an exception")

    return mock, process


def test_timeout_process(mock_process, user_dofn):

    dofn_inst = user_dofn()
    entity_id = "1234"

    tprocess = timeout_process.TimeoutProcess(
        target=mock_process,
        args=([dofn_inst, entity_id]),
        name="TimeoutProcess_{}_{}".format(
            dofn_inst._klio._transform_name, entity_id
        ),
    )

    assert hasattr(tprocess, "_pconn")
    assert hasattr(tprocess, "_cconn")
    assert hasattr(tprocess, "_exception")
    assert hasattr(tprocess, "_return_value")

    assert tprocess.name == "TimeoutProcess_{}_{}".format(
        dofn_inst._klio._transform_name, entity_id
    )

    assert isinstance(tprocess, multiprocessing.Process)

    run_process = getattr(tprocess, "run", None)
    assert run_process
    assert callable(run_process)

    saferun_process = getattr(tprocess, "saferun", None)
    assert saferun_process
    assert callable(saferun_process)


def test_timeout_process_no_target():
    tprocess = timeout_process.TimeoutProcess(
        target=None, args=(), name="TimeoutProcess_No_Target_Func"
    )

    ret_value = tprocess.saferun()
    assert ret_value is None


def test_run_timeout_process(mock_process, monkeypatch, user_dofn):

    dofn_inst = user_dofn()
    inner_mock, mock_p = mock_process
    tprocess = timeout_process.TimeoutProcess(
        target=mock_p, args=([dofn_inst, "1234"])
    )

    inner_mock.assert_not_called()

    monkeypatch.setattr(timeout_process.TimeoutProcess, "exitcode", 0)

    tprocess.run()

    inner_mock.assert_called_once_with(dofn_inst, "1234")

    assert not tprocess.exception
    assert "done" == tprocess.return_value


def test_run_timeout_process_generator(
    mock_process_generator, monkeypatch, user_dofn
):

    dofn_inst = user_dofn()
    inner_mock, mock_p = mock_process_generator
    tprocess = timeout_process.TimeoutProcess(
        target=mock_p, args=([dofn_inst, "1234"])
    )

    inner_mock.assert_not_called()

    monkeypatch.setattr(timeout_process.TimeoutProcess, "exitcode", 0)

    tprocess.run()

    inner_mock.assert_called_once_with(dofn_inst, "1234")
    assert not tprocess.exception
    assert "done" == tprocess.return_value


def test_run_timeout_process_exception(
    mock_process_raises, user_dofn, monkeypatch
):

    dofn_inst = user_dofn()
    inner_mock, mock_p = mock_process_raises
    tprocess = timeout_process.TimeoutProcess(
        target=mock_p, args=([dofn_inst, "1234"])
    )

    inner_mock.assert_not_called()

    monkeypatch.setattr(timeout_process.TimeoutProcess, "exitcode", 1)

    with pytest.raises(Exception):
        tprocess.run()

    inner_mock.assert_called_once_with(dofn_inst, "1234")

    assert not tprocess.return_value
    assert tprocess.exception


def test_timeout_process_exception_no_recv(mocker, monkeypatch):
    monkeypatch.setattr(timeout_process.TimeoutProcess, "exitcode", 1)
    tprocess = timeout_process.TimeoutProcess(
        target=None, args=(), name="TimeoutProcess_Test"
    )
    mock_pconn = mocker.Mock()
    mock_pconn.poll.return_value = False
    monkeypatch.setattr(tprocess, "_pconn", mock_pconn)

    assert tprocess.exception is None
    mock_pconn.poll.assert_called_once_with()
    mock_pconn.recv.assert_not_called()


def test_timeout_process_return_value_no_recv(mocker, monkeypatch):
    monkeypatch.setattr(timeout_process.TimeoutProcess, "exitcode", 0)
    tprocess = timeout_process.TimeoutProcess(
        target=None, args=(), name="TimeoutProcess_Test"
    )
    mock_pconn = mocker.Mock()
    mock_pconn.poll.return_value = False
    monkeypatch.setattr(tprocess, "_pconn", mock_pconn)

    assert tprocess.return_value is None
    mock_pconn.poll.assert_called_once_with()
    mock_pconn.recv.assert_not_called()

"""
Notice: the code below is just a simple example of how to write unit
tests for a transform.

Feel free to import what's needed, rewrite tests, etc.
"""
import pytest

from klio_core.proto import klio_pb2

import transforms


@pytest.fixture
def klio_msg():
    msg = klio_pb2.KlioMessage()
    msg.data.element = b"hello"
    msg.version = klio_pb2.Version.V2
    return msg.SerializeToString()


def test_process(klio_msg):
    """Assert process method yields expected data."""
    helloklio_dofn = transforms.HelloKlio()
    output = helloklio_fn.process(klio_msg)
    assert klio_msg == list(output)[0]

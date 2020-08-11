"""
Notice: the code below is just a simple example of how to write unit
tests for a transform.

Feel free to import what's needed, rewrite tests, etc.
"""
import pytest

import transforms


@pytest.fixture
def helloklio_dofn():
    """An instance of HelloKlio available for test functions.

    See https://docs.pytest.org/en/latest/fixture.html for more info
    on pytest fixtures.
    """
    return transforms.HelloKlio()


def test_process(helloklio_dofn):
    """`process` method returns expected message and logs expected lines."""
    input_entity_id = "message"
    expected = "message"
    assert expected == helloklio_dofn.process(input_entity_id)


def test_input_data_exists(helloklio_dofn):
    """Assert input data does exist."""
    input_exists = helloklio_dofn.input_data_exists("message")
    assert input_exists is True


def test_output_data_exists(helloklio_dofn):
    """Assert output data does not exist."""
    output_exists = helloklio_dofn.output_data_exists("message")
    assert output_exists is False

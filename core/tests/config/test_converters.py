# -*- coding: utf-8 -*-
#
# Copyright 2019 Spotify AB

import pytest

from klio_core import config
from klio_core.config import _converters as converters
from klio_core.config import _utils as utils


@utils.config_object(key_prefix="foo.bar")
class ConfigTestClass(object):
    f1 = utils.field(type=str, default=None)
    f2 = utils.field(type=str)


def test_config_decorator_direct_instantiation():
    a = ConfigTestClass(f1="f1value", f2=None)
    assert "f1value" == a.f1
    assert a.f2 is None

    b = ConfigTestClass(f2="value")
    assert b.f1 is None
    assert "value" == b.f2

    with pytest.raises(Exception):
        ConfigTestClass(f1="value")


@pytest.mark.parametrize(
    "config_dict, expected",
    (
        (
            {"f1": "f1value", "f2": None},
            ConfigTestClass(f1="f1value", f2=None),
        ),
        ({"f1": "f1value"}, None),
        ({"f2": "value"}, ConfigTestClass(f1=None, f2="value")),
        ({}, None),
    ),
)
def test_config_decorator_no_value(config_dict, expected):
    if expected:
        assert expected == ConfigTestClass(config_dict)
    else:
        with pytest.raises(Exception):
            ConfigTestClass(config_dict)


@pytest.mark.parametrize(
    "value, expected", ((5, "5"), ("foo", "foo"), (None, None), (True, "True"))
)
def test_str_converter(value, expected):
    assert expected == converters.StringConverter("foo").validate(value)


@pytest.mark.parametrize(
    "bad_value", (converters.UNSET_REQUIRED_VALUE, {}, [])
)
def test_str_converter_raises(bad_value):
    with pytest.raises(Exception):
        converters.StringConverter("foo").validate(bad_value)


@pytest.mark.parametrize(
    "value, expected",
    (
        (5, True),
        (True, True),
        (None, None),
        (0, False),
        (False, False),
        ("true", True),
        ("false", True),  # hmmmmm
    ),
)
def test_bool_converter(value, expected):
    assert expected == converters.BoolConverter("foo").validate(value)


@pytest.mark.parametrize(
    "bad_value", (converters.UNSET_REQUIRED_VALUE, {}, [])
)
def test_bool_converter_raises(bad_value):
    with pytest.raises(Exception):
        converters.BoolConverter("foo").validate(bad_value)


@pytest.mark.parametrize(
    "value, expected", ((5, 5), ("5", 5), (None, None), (True, 1))
)
def test_int_converter(value, expected):
    assert expected == converters.IntConverter("foo").validate(value)


@pytest.mark.parametrize(
    "bad_value", ("3.14", converters.UNSET_REQUIRED_VALUE, "something", {}, [])
)
def test_int_converter_raises(bad_value):
    with pytest.raises(Exception):
        config.IntConverter("foo").validate(bad_value)

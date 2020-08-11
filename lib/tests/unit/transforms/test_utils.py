# Copyright 2020 Spotify AB

import pytest

from klio.transforms import _utils
from klio.transforms import core as core_transforms


class BaseKlassMeta(object):
    __metaclass__ = core_transforms.KlioDoFnMetaclass


class KlassMeta(BaseKlassMeta):
    def process(self):
        pass

    def not_process(self):
        pass


@pytest.mark.parametrize(
    "clsdict,bases,expected",
    (
        (
            {"process": KlassMeta.process},
            (core_transforms.KlioBaseDoFn,),
            True,
        ),
        (
            {"process": KlassMeta.not_process},
            (core_transforms.KlioBaseDoFn,),
            False,
        ),
        (
            {"process": "not a callable"},
            (core_transforms.KlioBaseDoFn,),
            False,
        ),
        ({"process": KlassMeta.process}, (BaseKlassMeta,), False),
        ({}, (BaseKlassMeta,), False),
    ),
)
def test_is_original_process_func(clsdict, bases, expected):
    actual = _utils.is_original_process_func(
        clsdict, bases, base_class="KlioBaseDoFn"
    )

    assert expected == actual

# Copyright 2020 Spotify AB

import pytest

from klio.transforms import _helpers
from klio.transforms import _utils


class BaseKlassMeta(object):
    __metaclass__ = _helpers._KlioBaseDataExistenceCheck


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
            (_helpers._KlioBaseDataExistenceCheck,),
            True,
        ),
        (
            {"process": KlassMeta.not_process},
            (_helpers._KlioBaseDataExistenceCheck,),
            False,
        ),
        (
            {"process": "not a callable"},
            (_helpers._KlioBaseDataExistenceCheck,),
            False,
        ),
        ({"process": KlassMeta.process}, (BaseKlassMeta,), False),
        ({}, (BaseKlassMeta,), False),
    ),
)
def test_is_original_process_func(clsdict, bases, expected):
    actual = _utils.is_original_process_func(
        clsdict, bases, base_class="_KlioBaseDataExistenceCheck"
    )

    assert expected == actual

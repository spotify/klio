# Copyright 2020 Spotify AB
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

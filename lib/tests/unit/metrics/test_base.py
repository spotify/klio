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
import six

from klio.metrics import base as base_metrics


class FakeObject(object):
    pass


@pytest.mark.parametrize("obj", [None, FakeObject()])
def test_abstract_attr(obj):
    ret_obj = base_metrics.abstract_attr(obj)

    if obj:
        assert isinstance(ret_obj, FakeObject)
    else:
        assert isinstance(ret_obj, base_metrics._DummyAttribute)

    assert hasattr(ret_obj, "__isabstractattr__")


def test__has_abstract_attributes_implemented_decorator():
    class TestMeta(six.with_metaclass(base_metrics._ABCBaseMeta)):
        @property
        @base_metrics.abstract_attr
        def foo(self):
            pass

    class TestABC(TestMeta):
        foo = "bar"

    rc = TestABC()
    assert hasattr(rc, "foo")


def test_has_abstract_attributes_implemented_cls_attr():
    class TestMeta(six.with_metaclass(base_metrics._ABCBaseMeta)):
        foo = base_metrics.abstract_attr()

    class TestABC(TestMeta):
        foo = "bar"

    rc = TestABC()
    assert hasattr(rc, "foo")


def test_has_abstract_attributes_implemented_raises():
    class TestMeta(six.with_metaclass(base_metrics._ABCBaseMeta)):
        foo = base_metrics.abstract_attr()

    with pytest.raises(NotImplementedError):

        class TestABC(TestMeta):
            not_foo = "nope"


def test_abstract_relay_client():
    class RelayClient(base_metrics.AbstractRelayClient):
        RELAY_CLIENT_NAME = "test-relay-client"

        def unmarshal(self, metric):
            return {}

        def emit(self, metric):
            return {}

        def counter(self, name, value=0, transform=None):
            return {}

        def gauge(self, name, value=0, transform=None):
            return {}

        def timer(self, name, value=0, transform=None):
            return {}

    rc = RelayClient({})
    assert hasattr(rc, "RELAY_CLIENT_NAME")
    assert hasattr(rc, "unmarshal")
    assert hasattr(rc, "emit")
    assert hasattr(rc, "counter")
    assert hasattr(rc, "gauge")
    assert hasattr(rc, "timer")


def test_abstract_relay_client_raises():
    with pytest.raises(NotImplementedError):

        class RelayClient(base_metrics.AbstractRelayClient):
            pass


def test_base_metric():
    class MyMetric(base_metrics.BaseMetric):
        def __init__(self, name, value=0, transform=None, tags=None, **kwargs):
            super(MyMetric, self).__init__(
                name, value=value, transform=transform
            )
            self.tags = tags

    my_metric_inst = MyMetric(name="foo")

    assert isinstance(my_metric_inst, base_metrics.BaseMetric)
    assert hasattr(my_metric_inst, "name")
    assert hasattr(my_metric_inst, "value")
    assert hasattr(my_metric_inst, "transform")
    assert hasattr(my_metric_inst, "update")

    assert 0 == my_metric_inst.value
    my_metric_inst.update(1)
    assert 1 == my_metric_inst.value

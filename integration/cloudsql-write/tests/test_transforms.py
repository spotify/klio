# Copyright 2021 Spotify AB
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
    output = helloklio_dofn.process(klio_msg)
    assert klio_msg == list(output)[0]
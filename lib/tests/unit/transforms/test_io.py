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

import glob
import json
import os
import tempfile

import apache_beam as beam

from apache_beam.testing import test_pipeline

from klio_core.proto import klio_pb2

from klio.transforms import io as io_transforms


HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, os.path.pardir, "fixtures")


def assert_expected_klio_msg_from_file(element):
    message = klio_pb2.KlioMessage()
    message.ParseFromString(element)
    assert message.data.element is not None
    assert isinstance(message.data.element, bytes)


def test_read_from_file():
    file_path = os.path.join(FIXTURE_PATH, "elements_text_file.txt")

    transform = io_transforms.KlioReadFromText(file_path)
    with test_pipeline.TestPipeline() as p:
        (
            p
            | "Read" >> transform
            | beam.Map(assert_expected_klio_msg_from_file)
        )

    assert transform._REQUIRES_IO_READ_WRAP is False


def test_write_to_file():
    file_path_read = os.path.join(FIXTURE_PATH, "elements_text_file.txt")

    with tempfile.TemporaryDirectory() as tmp_path:
        with test_pipeline.TestPipeline() as p:
            (
                p
                | io_transforms.KlioReadFromText(file_path_read)
                | io_transforms.KlioWriteToText(tmp_path)
            )
        # WriteToText will shard files so we iterate through each
        # file in the directory
        write_results = []

        for file_name in glob.glob(tmp_path + "*"):
            if os.path.isfile(os.path.join(tmp_path, file_name)):
                with open(file_name, "rb") as f:
                    write_results.extend(f.readlines())
        with open(file_path_read, "rb") as fr:
            read_results = fr.readlines()
        assert write_results == read_results


def _expected_avro_kmsgs():
    expected_records = [
        {
            "username": "miguno",
            "tweet": "Rock: Nerf paper, scissors is fine.",
            "timestamp": 1366150681,
        },
        {
            "username": "BlizzardCS",
            "tweet": "Works as intended.  Terran is IMBA.",
            "timestamp": 1366154481,
        },
    ]
    expected_kmsgs = []
    for record in expected_records:
        message = klio_pb2.KlioMessage()
        message.version = klio_pb2.Version.V2
        message.metadata.intended_recipients.anyone.SetInParent()
        message.data.element = bytes(json.dumps(record).encode("utf-8"))
        expected_kmsgs.append(message)
    return expected_kmsgs


def assert_expected_klio_msg_from_avro(element):
    expected_kmsgs = _expected_avro_kmsgs()
    message = klio_pb2.KlioMessage()
    message.ParseFromString(element)
    assert message in expected_kmsgs


def test_read_from_avro():
    file_pattern = os.path.join(FIXTURE_PATH, "twitter.avro")

    with test_pipeline.TestPipeline() as p:
        (
            p
            | io_transforms.KlioReadFromAvro(file_pattern=file_pattern)
            | beam.Map(assert_expected_klio_msg_from_avro)
        )

    assert io_transforms.KlioReadFromAvro._REQUIRES_IO_READ_WRAP is True

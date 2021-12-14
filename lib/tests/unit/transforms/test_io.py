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
from unittest import mock

import apache_beam as beam
import pytest

from apache_beam.testing import test_pipeline
from apache_beam.testing import util as btest_util

from klio_core.proto import klio_pb2

from klio.transforms import core
from tests.unit import conftest

# NOTE: All of the Klio IO transforms use _KlioIOCounter, which instantiates a
# KlioContext object.  Since all IO transforms use the _KlioIOCounter, we just
# patch on the module level instead of within each and every test function.
patcher = mock.patch.object(core.RunConfig, "get", conftest._klio_config)
patcher.start()
from klio.transforms import io as io_transforms  # NOQA E402

patcher.stop()

HERE = os.path.abspath(os.path.join(os.path.abspath(__file__), os.path.pardir))
FIXTURE_PATH = os.path.join(HERE, os.path.pardir, "fixtures")


def assert_expected_klio_msg_from_file(element):
    message = klio_pb2.KlioMessage()
    message.ParseFromString(element)
    assert message.data.element is not None
    assert isinstance(message.data.element, bytes)


@mock.patch.object(core.RunConfig, "get", conftest._klio_config)
def test_read_from_file():
    file_path = os.path.join(FIXTURE_PATH, "elements_text_file.txt")

    exp_element_count = 0
    with open(file_path, "r") as f:
        exp_element_count = len(f.readlines())

    transform = io_transforms.KlioReadFromText(file_path)
    with test_pipeline.TestPipeline() as p:
        (
            p
            | "Read" >> transform
            | beam.Map(assert_expected_klio_msg_from_file)
        )

    assert transform._reader._REQUIRES_IO_READ_WRAP is False

    actual_counters = p.result.metrics().query()["counters"]
    assert 1 == len(actual_counters)
    assert exp_element_count == actual_counters[0].committed
    assert "KlioReadFromText" == actual_counters[0].key.metric.namespace
    assert "kmsg-read" == actual_counters[0].key.metric.name


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
        exp_element_count = 0
        for file_name in glob.glob(tmp_path + "*"):
            if os.path.isfile(os.path.join(tmp_path, file_name)):
                with open(file_name, "rb") as f:
                    lines = f.readlines()
                    exp_element_count = len(lines)
                    write_results.extend(lines)
        with open(file_path_read, "rb") as fr:
            read_results = fr.readlines()
        assert write_results == read_results

    actual_counters = p.result.metrics().query()["counters"]
    assert 2 == len(actual_counters)

    read_counter = actual_counters[0]
    write_counter = actual_counters[1]
    assert exp_element_count == read_counter.committed
    assert "KlioReadFromText" == read_counter.key.metric.namespace
    assert "kmsg-read" == read_counter.key.metric.name

    assert exp_element_count == write_counter.committed
    assert "KlioWriteToText" == write_counter.key.metric.namespace
    assert "kmsg-write" == write_counter.key.metric.name


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

    transform = io_transforms.KlioReadFromAvro(file_pattern=file_pattern)
    with test_pipeline.TestPipeline() as p:
        p | transform | beam.Map(assert_expected_klio_msg_from_avro)

    assert transform._reader._REQUIRES_IO_READ_WRAP is True

    actual_counters = p.result.metrics().query()["counters"]
    assert 1 == len(actual_counters)
    assert 2 == actual_counters[0].committed
    assert "KlioReadFromAvro" == actual_counters[0].key.metric.namespace
    assert "kmsg-read" == actual_counters[0].key.metric.name


def assert_expected_klio_msg_from_avro_write(element):
    file_path_read = os.path.join(FIXTURE_PATH, "elements_text_file.txt")
    with open(file_path_read, "rb") as fr:
        expected_elements = fr.read().splitlines()
    message = klio_pb2.KlioMessage()
    message.ParseFromString(element)
    assert message.data.element in expected_elements


def test_write_to_avro():

    file_path_read = os.path.join(FIXTURE_PATH, "elements_text_file.txt")
    exp_element_count = 0
    with open(file_path_read, "r") as f:
        exp_element_count = len(f.readlines())

    with tempfile.TemporaryDirectory() as tmp_path:
        with test_pipeline.TestPipeline() as p:

            p | io_transforms.KlioReadFromText(
                file_path_read
            ) | io_transforms.KlioWriteToAvro(file_path_prefix=tmp_path)

        files = glob.glob(tmp_path + "*")
        assert len(files) > 0
        assert (
            os.path.isfile(os.path.join(tmp_path, file_name))
            for file_name in files
        )

        with test_pipeline.TestPipeline() as p2:
            p2 | io_transforms.KlioReadFromAvro(
                file_pattern=(tmp_path + "*")
            ) | beam.Map(assert_expected_klio_msg_from_avro_write)

    actual_counters = p.result.metrics().query()["counters"]
    assert 2 == len(actual_counters)

    read_counter = actual_counters[0]
    write_counter = actual_counters[1]

    assert exp_element_count == read_counter.committed
    assert "KlioReadFromText" == read_counter.key.metric.namespace
    assert "kmsg-read" == read_counter.key.metric.name

    assert exp_element_count == write_counter.committed
    assert "KlioWriteToAvro" == write_counter.key.metric.namespace
    assert "kmsg-write" == write_counter.key.metric.name


def test_avro_io_immutability():

    initial_data_path = os.path.join(FIXTURE_PATH, "twitter.avro")

    with tempfile.TemporaryDirectory() as tmp_path:
        with test_pipeline.TestPipeline() as p:

            p | io_transforms.KlioReadFromAvro(
                initial_data_path
            ) | io_transforms.KlioWriteToAvro(
                file_path_prefix=tmp_path, num_shards=0
            )

        with test_pipeline.TestPipeline() as p2:

            p2 | io_transforms.KlioReadFromAvro(
                file_pattern=tmp_path + "*"
            ) | beam.Map(assert_expected_klio_msg_from_avro)


def test_bigquery_mapper_generate_klio_message():

    mapper = io_transforms._KlioReadFromBigQueryMapper()
    message = mapper._generate_klio_message()

    assert message.version == klio_pb2.Version.V2
    assert (
        message.metadata.intended_recipients.WhichOneof("recipients")
        == "anyone"
    )


@pytest.mark.parametrize(
    "klio_message_columns,row,expected",
    (
        (["one_column"], {"a": "A", "b": "B", "one_column": "value"}, "value"),
        (
            ["a", "b"],
            {"a": "A", "b": "B", "c": "C"},
            json.dumps({"a": "A", "b": "B"}),
        ),
        (None, {"a": "A", "b": "B"}, json.dumps({"a": "A", "b": "B"})),
    ),
)
def test_bigquery_mapper_map_row_element(klio_message_columns, row, expected):
    mapper = io_transforms._KlioReadFromBigQueryMapper(
        klio_message_columns=klio_message_columns
    )

    actual = mapper._map_row_element(row)

    assert actual == expected


def _generate_kmsg(element):
    message = klio_pb2.KlioMessage()
    message.version = klio_pb2.Version.V2
    message.metadata.intended_recipients.anyone.SetInParent()
    message.data.element = bytes(json.dumps(element), "utf-8")
    return message.SerializeToString()


def test_read_from_bigquery(monkeypatch):
    read_from_bq = io_transforms.KlioReadFromBigQuery()
    monkeypatch.setattr(read_from_bq, "_reader", beam.Map(lambda x: x))

    table_data = [{"entity_id": 1}, {"entity_id": 2}, {"entity_id": 3}]
    expected_pcoll = [_generate_kmsg(k) for k in table_data]
    with test_pipeline.TestPipeline() as p:
        act_pcoll = p | beam.Create(table_data) | read_from_bq
        btest_util.assert_that(act_pcoll, btest_util.equal_to(expected_pcoll))

    actual_counters = p.result.metrics().query()["counters"]
    assert 1 == len(actual_counters)
    assert len(table_data) == actual_counters[0].committed
    assert "KlioReadFromBigQuery" == actual_counters[0].key.metric.namespace
    assert "kmsg-read" == actual_counters[0].key.metric.name


def _generate_kmsg_with_payload(element):
    message = klio_pb2.KlioMessage()
    message.version = klio_pb2.Version.V2
    message.metadata.intended_recipients.anyone.SetInParent()
    message.data.element = bytes(str(element["entity_id"]), "utf-8")
    message.data.payload = bytes(json.dumps(element), "utf-8")
    return message.SerializeToString()


def test_write_to_bigquery(monkeypatch):
    write_to_bq = io_transforms.KlioWriteToBigQuery(
        project="test-project", dataset="test-dataset", table="test-table"
    )
    monkeypatch.setattr(write_to_bq, "_writer", beam.Map(lambda x: x))

    table_data = [{"entity_id": 1}, {"entity_id": 2}, {"entity_id": 3}]
    with test_pipeline.TestPipeline() as p:
        act_pcoll = (
            p
            | beam.Create(table_data)
            | beam.Map(_generate_kmsg_with_payload)
            | write_to_bq
        )
        btest_util.assert_that(act_pcoll, btest_util.equal_to(table_data))

    actual_counters = p.result.metrics().query()["counters"]
    assert 1 == len(actual_counters)
    assert len(table_data) == actual_counters[0].committed
    assert "KlioWriteToBigQuery" == actual_counters[0].key.metric.namespace
    assert "kmsg-write" == actual_counters[0].key.metric.name

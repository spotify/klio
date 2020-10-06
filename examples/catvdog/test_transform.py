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

from klio_core import config
from klio_core.proto import klio_pb2

import transforms


@pytest.fixture(autouse=True)
def mock_config(mocker, monkeypatch):
    mock = mocker.Mock(autospec=config.KlioConfig)
    monkeypatch.setattr(transforms.CatVDog, "_klio.config", mock, raising=False)
    return mock


@pytest.fixture(autouse=True)
def mock_gcs_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(transforms.gcsio, "GcsIO", mock)
    return mock


@pytest.fixture
def mock_model(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(transforms.tf.keras.models, "load_model", mock)
    return mock


@pytest.fixture
def klio_msg():
    msg = klio_pb2.KlioMessage()
    msg.version = klio_pb2.Version.V2
    msg.data.element = b"1234"
    return msg.SerializeToString()


@pytest.mark.parametrize("prediction,exp_folder", ((0, "cat"), (1, "dog")))
def test_process(
    prediction, exp_folder, klio_msg, mock_model, mocker, monkeypatch
):
    mock_model.return_value.predict_classes.return_value = [[prediction]]

    mock_download_image = mocker.Mock()
    mock_download_image.return_value.name = "/tmp/tmp_abcd.jpg"
    monkeypatch.setattr(
        transforms.CatVDog, "download_image", mock_download_image
    )

    mock_load_image = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "load_image", mock_load_image)

    mock_upload_image = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "upload_image", mock_upload_image)

    dofn_inst = transforms.CatVDog()
    dofn_inst.setup()
    ret_data = next(dofn_inst.process(klio_msg))

    filename = "1234.jpg"
    mock_download_image.assert_called_once_with(filename)
    mock_load_image.assert_called_once_with(
        mock_download_image.return_value.name
    )
    mock_model.return_value.predict_classes.assert_called_once_with(
        mock_load_image.return_value
    )
    mock_upload_image.assert_called_once_with(
        mock_download_image.return_value, exp_folder, filename
    )
    assert klio_msg == ret_data

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

import transforms


@pytest.fixture
def mock_gcs_client(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "gcs_client", mock)
    return mock


@pytest.fixture
def mock_model(mocker, monkeypatch):
    mock = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "model", mock)
    return mock


@pytest.mark.parametrize("prediction,exp_folder", ((0, "cat"), (1, "dog")))
def test_process(prediction, exp_folder, mock_model, mocker, monkeypatch):
    mock_model.predict_classes.return_value = [[prediction]]

    mock_input_exists = mocker.Mock()
    monkeypatch.setattr(
        transforms.CatVDog, "input_data_exists", mock_input_exists
    )

    mock_output_exists = mocker.Mock()
    monkeypatch.setattr(
        transforms.CatVDog, "output_data_exists", mock_output_exists
    )

    mock_download_image = mocker.Mock()
    mock_download_image.return_value = "/tmp/tmp_abcd.jpg"
    monkeypatch.setattr(
        transforms.CatVDog, "download_image", mock_download_image
    )

    mock_load_image = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "load_image", mock_load_image)

    mock_upload_image = mocker.Mock()
    monkeypatch.setattr(transforms.CatVDog, "upload_image", mock_upload_image)

    dofn_inst = transforms.CatVDog()
    entity_id = "1234"
    ret_entity_id = dofn_inst.process(entity_id)

    filename = "1234.jpg"
    mock_download_image.assert_called_once_with(filename)
    mock_load_image.assert_called_once_with(mock_download_image.return_value)
    mock_model.predict_classes.assert_called_once_with(
        mock_load_image.return_value
    )
    mock_upload_image.assert_called_once_with(
        mock_download_image.return_value, exp_folder, filename
    )
    assert entity_id == ret_entity_id


@pytest.mark.parametrize("input_exists", (True, False))
def test_input_data_exists(input_exists, mock_gcs_client, monkeypatch):
    mock_gcs_client.exists.return_value = input_exists

    input_location = "gs://foo/bar"
    entity_id = "1234"

    dofn = transforms.CatVDog()
    monkeypatch.setattr(dofn, "input_loc", input_location)

    actual_input_exists = dofn.input_data_exists(entity_id)

    assert input_exists == actual_input_exists
    mock_gcs_client.exists.assert_called_once_with(
        "{}/{}.jpg".format(input_location, entity_id)
    )


@pytest.mark.parametrize(
    "output_exists,exp_exists,exp_call_count",
    (
        # exists for cat
        ([False, True], True, 2),
        # exists for dog
        ([True, False], True, 1),
        # does not exist
        ([False, False], False, 2),
    ),
)
def test_output_data_exists(
    output_exists, exp_exists, exp_call_count, mock_gcs_client, monkeypatch
):
    mock_gcs_client.exists.side_effect = output_exists

    output_location = "gs://foo/bar"
    entity_id = "1234"

    dofn = transforms.CatVDog()
    monkeypatch.setattr(dofn, "output_loc", output_location)

    actual_output_exists = dofn.output_data_exists(entity_id)

    assert exp_exists == actual_output_exists
    assert exp_call_count == mock_gcs_client.exists.call_count

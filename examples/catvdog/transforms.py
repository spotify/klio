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

import os
import tempfile

import apache_beam as beam
import numpy as np
import tensorflow as tf

from apache_beam.io.gcp import gcsio
from keras.models import load_model
from keras.preprocessing import image as kimage

from klio.transforms import decorators


class CatVDog(beam.DoFn):
    """Classify cat vs dog based off github.com/gsurma/image_classifier"""

    IMAGE_WIDTH = 200
    IMAGE_HEIGHT = 200
    CLASSES = {0: "cat", 1: "dog"}

    @decorators.set_klio_context
    def __init__(self):
        self.input_loc = self._klio.config.job_config.data.inputs[0].location
        self.output_loc = self._klio.config.job_config.data.outputs[0].location
        self.model_file = self._klio.config.job_config.as_dict()["model_file"]
        self.gcs_client = None
        self.model = None

    def setup(self):
        """Setup instance variables that are not pickle-able"""
        self.gcs_client = gcsio.GcsIO()
        self.model = tf.keras.models.load_model(self.model_file)

    @decorators.set_klio_context
    def download_image(self, filename):
        """Download a given image from GCS.

        Args:
            filename (str): filename to download from configured GCS bucket.
        Returns:
            (tempfile.NamedTemporaryFile) Temporary file object of the
                downloaded image.
        """
        remote_file = os.path.join(self.input_loc, filename)

        local_tmp_file = tempfile.NamedTemporaryFile(suffix=".jpg")
        with self.gcs_client.open(remote_file, "rb") as source:
            with open(local_tmp_file.name, "wb") as dest:
                dest.write(source.read())
        self._klio.logger.info("Downloaded file to %s" % local_tmp_file.name)
        return local_tmp_file

    def load_image(self, image_file):
        """Load a given image for classification.

        Args:
            image_file (tempfile.NamedTemporaryFile): Temporary image
                file object with which to load.
        Returns:
            (numpy.ndarray) loaded image tensor.
        """
        # Adapted from https://stackoverflow.com/a/47341572/1579977
        img = kimage.load_img(
            image_file,
            target_size=(CatVDog.IMAGE_WIDTH, CatVDog.IMAGE_HEIGHT),
        )
        img_tensor = kimage.img_to_array(img)
        img_tensor = np.expand_dims(img_tensor, axis=0)
        img_tensor /= 255.0

        return img_tensor

    @decorators.set_klio_context
    def upload_image(self, local_file, classification, filename):
        """Upload a given image to GCS.

        Args:
            local_file (tempfile.NamedTemporaryFile): Temporary image
                file object with which to load.
            classification (str): which classification subfolder to
                upload local_file to.
            filename (str): name for the uploaded file.
        """
        remote_dir = os.path.join(self.output_loc, classification, filename)
        with self.gcs_client.open(remote_dir, "wb") as dest:
            with open(local_file.name, "rb") as source:
                dest.write(source.read())
        self._klio.logger.info("Uploaded file to %s" % remote_dir)

    @decorators.handle_klio
    def process(self, data):
        """Predict whether a given image ID is a cat or a dog.

        This is the main entry point for a Beam/Klio transform.

        Download the image, file, make a prediction, then upload image
        to its classified folder in a GCS bucket.

        Args:
            data (KlioMessage.data): data attribute of a KlioMessage including
                fields ``element`` and ``payload``.
        Returns:
            data (KlioMessage.data): data attribute of the incoming KlioMessage
            as there is no need to pass state to the downstream transform
            within the pipeline.
        """
        image_id = data.element.decode("utf-8")
        self._klio.logger.info("Received {} from PubSub".format(image_id))
        filename = "{}.jpg".format(image_id)

        # download image
        input_file = self.download_image(filename)

        # load & predict image
        loaded_image = self.load_image(input_file.name)
        prediction = self.model.predict_classes(loaded_image)
        prediction = CatVDog.CLASSES[prediction[0][0]]
        self._klio.logger.info(
            "Predicted {} for {}".format(prediction, image_id)
        )

        # save image to particular output directory
        self.upload_image(input_file, prediction, filename)

        # return original data for any downstream processing
        yield data


class CatVDogOutputCheck(beam.DoFn):
    """Custom output data existence check to handle two output directories"""

    def setup(self):
        """Setup instance variables that are not pickle-able"""
        self.gcs_client = gcsio.GcsIO()

    @decorators.handle_klio
    def process(self, data):
        """Detect if data for an element exists in one of two dirs in a bucket.

        Args:
            data (KlioMessage.data): data attribute of a KlioMessage including
                fields ``element`` and ``payload``.
        Returns:
            apache_beam.pvalue.TaggedOutput: data tagged with either
            ``found`` or ``not_found``.
        """
        element = data.element.decode("utf-8")

        oc = self._klio.config.job_config.data.outputs[0]
        subdirs = ("cat", "dog")

        outputs_exist = []

        for subdir in subdirs:
            path = f"{oc.location}/{subdir}/{element}{oc.file_suffix}"
            self._klio.logger.info(f"Checking output in {path}")
            exists = self.gcs_client.exists(path)
            outputs_exist.append(exists)
            if exists:
                self._klio.logger.info(f"Output exists at {path}")
            else:
                self._klio.logger.info(
                    f"Output does not exist for {element} in {subdir}"
                )

        if any(outputs_exist):
            yield beam.pvalue.TaggedOutput("found", data)
        else:
            yield beam.pvalue.TaggedOutput("not_found", data)

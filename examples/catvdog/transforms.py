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

import enum
import os
import tempfile
import threading

import numpy as np
import tensorflow as tf
from apache_beam.io.gcp import gcsio
from keras.models import load_model
from keras.preprocessing import image as kimage

from klio import transforms


class CatVDog(transforms.KlioBaseDoFn):
    """Classify cat vs dog based off github.com/gsurma/image_classifier"""

    IMAGE_WIDTH = 200
    IMAGE_HEIGHT = 200
    CLASSES = {0: "cat", 1: "dog"}
    _thread_local = threading.local()

    def __init__(self):
        self.input_loc = self._klio.config.job_config.inputs[0].data_location
        self.output_loc = self._klio.config.job_config.outputs[0].data_location
        self.model_file = self._klio.config.job_config.as_dict()["model_file"]

    @property
    def gcs_client(self):
        # Note: DirectRunner does not support a `setup` method. If just
        # running with DataflowRunner, this can be moved into a `setup`
        # method without any threadlocal lookups.
        client = getattr(self._thread_local, "gcs_client", None)
        if not client:
            self._thread_local.gcs_client = gcsio.GcsIO()
        return self._thread_local.gcs_client

    @property
    def model(self):
        # Note: DirectRunner does not support a `setup` method. If just
        # running with DataflowRunner, this can be moved into a `setup`
        # method without any threadlocal lookups.
        m = getattr(self._thread_local, "model", None)
        if not m:
            self._thread_local.model = tf.keras.models.load_model(
                self.model_file
            )
        return self._thread_local.model

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

    def process(self, entity_id):
        """Predict whether a given image ID is a cat or a dog.

        This is the main entry point for a Beam/Klio transform.

        Download the image, file, make a prediction, then upload image
        to its classified folder in a GCS bucket.

        Args:
            entity_id (str): unique file identifier of an image to
                classify.
        Returns:
            entity_id (str): unique file identifier of an image that has
                been classified.
        """
        self._klio.logger.info("Received {} from PubSub".format(entity_id))
        filename = "{}.jpg".format(entity_id)

        # download image
        input_file = self.download_image(filename)

        # load & predict image
        loaded_image = self.load_image(input_file)
        prediction = self.model.predict_classes(loaded_image)
        prediction = CatVDog.CLASSES[prediction[0][0]]
        self._klio.logger.info(
            "Predicted {} for {}".format(prediction, entity_id)
        )

        # save image to particular output directory
        self.upload_image(input_file, prediction, filename)

        # return original entity ID for downstream processing
        return entity_id

    def input_data_exists(self, entity_id):
        """Check if input data exists in GCS for a given entity ID.

        Before invoking the `process` method with the received
        `entity_id`, klio will call this method. If input data does not
        exist, klio will trigger the configured parent job (if any).

        Args:
            entity_id (str): unique image file ID.
        Returns
            (bool): whether or not image exists in the configured
                input bucket.
        """
        filename = "{}.jpg".format(entity_id)
        filepath = os.path.join(self.input_loc, filename)
        exists = self.gcs_client.exists(filepath)
        return exists

    def output_data_exists(self, entity_id):
        """Check if output data exists in GCS for a given entity ID.

        Before invoking the `process` method with the received
        `entity_id`, klio will call this method. If output data already
        exists and the `--force` flag from `klio message publish` was
        not provided, klio will not invoke the `process` method and
        effectively pass through the `entity_id` to the output pubsub
        topic.

        Args:
            entity_id (str): unique image file ID.
        Returns
            (bool): whether or not image exists in the configured
                output bucket.
        """
        filename = "{}.jpg".format(entity_id)
        dog_filepath = os.path.join(self.output_loc, "dog", filename)
        if self.gcs_client.exists(dog_filepath):
            return True

        cat_filepath = os.path.join(self.output_loc, "cat", filename)
        return self.gcs_client.exists(cat_filepath)

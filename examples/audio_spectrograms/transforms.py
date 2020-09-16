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
"""
Implement custom transforms using utilities for Klio-fication.
"""

import io
import os
import pickle

import apache_beam as beam
from apache_beam import pvalue

import librosa
import numpy as np

from klio_audio import decorators as audio_decorators
from klio_core.proto import klio_pb2
from klio.transforms import decorators as tfm_decorators



####
# Helper funcs for handling klio & numpy de/serialization when working
# with pcolls that are grouped by key
####
def _unpickle_from_klio_message(item):
    kmsg = klio_pb2.KlioMessage()
    kmsg.ParseFromString(item)
    return pickle.loads(kmsg.data.payload)


def _dump_to_klio_message(key, payload):
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = key
    out = io.BytesIO()
    np.save(out, payload)
    kmsg.data.payload = out.getvalue()
    return kmsg.SerializeToString()


#####
# Transforms
#####
class GetMagnitude(beam.DoFn):
    """Get the magnitude of a song given its STFT."""

    @tfm_decorators.handle_klio
    @audio_decorators.handle_binary(load_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Computing the magnitude spectrogram for {}".format(element)
        )
        stft = item.payload
        spectrogram, phase = librosa.magphase(stft)
        yield pvalue.TaggedOutput("phase", phase)
        yield pvalue.TaggedOutput("spectrogram", spectrogram)


class FilterNearestNeighbors(beam.DoFn):
    @tfm_decorators.handle_klio
    @audio_decorators.handle_binary
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Filtering nearest neighbors for {}".format(element)
        )
        spectrogram = item.payload
        nn_filter = librosa.decompose.nn_filter(
            spectrogram,
            aggregate=np.median,
            metric="cosine",
            width=int(librosa.time_to_frames(2)),
        )

        # The output of the filter shouldn't be greater than the input
        # if we assume signals are additive.  Taking the pointwise minimium
        # with the input spectrum forces this.
        nn_filter = np.minimum(spectrogram, nn_filter)
        yield nn_filter


class GetSoftMask(beam.DoFn):
    def __init__(self, margin=1, power=2):
        self.margin = margin
        self.power = power

    @tfm_decorators.set_klio_context
    def process(self, item):
        key, data = item
        first_data = data["first"][0]
        second_data = data["second"][0]
        full_data = data["full"][0]

        first = _unpickle_from_klio_message(first_data)
        second = _unpickle_from_klio_message(second_data)
        full = _unpickle_from_klio_message(full_data)

        self._klio.logger.debug("Getting softmask for {}".format(key))
        mask = librosa.util.softmask(
            first, self.margin * second, power=self.power
        )
        ret = mask * full
        yield _dump_to_klio_message(key, ret)


def create_key_from_element(item):
    kmsg = klio_pb2.KlioMessage()
    kmsg.ParseFromString(item)
    return (kmsg.data.element, item)


# key_pair looks like
# (element, {"full": [<serialized numpy array>],
#  "nnfilter": [<serialized numpy array>]})
def subtract_filter_from_full(key_pair):
    key, pair_data = key_pair
    full = _unpickle_from_klio_message(pair_data["full"][0])
    nn_filter = _unpickle_from_klio_message(pair_data["nnfilter"][0])

    net = full - nn_filter
    payload = pickle.dumps(net)
    kmsg = klio_pb2.KlioMessage()
    kmsg.data.element = key
    kmsg.data.payload = payload

    return (key, kmsg.SerializeToString())

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

import apache_beam as beam
import matplotlib.pyplot as plt

from klio.transforms import decorators as tfm_decorators

from klio_audio import decorators


class KlioAudioDoFnMetaclass(type):
    """Enforce behavior upon subclasses of `KlioAudioBaseDoFn`."""

    def __call__(self, *args, **kwargs):
        # automatically wrap DoFn in a beam.ParDo so folks can just do
        # `pcoll | SomeAudioTransform()` rather than
        # `pcoll | beam.ParDo(SomeAudioTransform())`
        return beam.ParDo(
            super(KlioAudioDoFnMetaclass, self).__call__(*args, **kwargs)
        )


class KlioAudioBaseDoFn(beam.DoFn, metaclass=KlioAudioDoFnMetaclass):
    pass


class KlioPlotBaseDoFn(KlioAudioBaseDoFn):
    DEFAULT_TITLE = ""

    def __init__(self, *_, title=None, **plot_args):
        if "ax" in plot_args:
            raise RuntimeError(
                "Invalid keyword `ax`: Specifying the plot's axes is not "
                "supported."
            )

        self.title = title or self.DEFAULT_TITLE
        self.plot_args = plot_args

    def _plot(self, *args, **kwargs):
        pass

    @tfm_decorators._handle_klio
    @decorators.handle_binary(load_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        title = self.title.format(element=element)
        self._klio.logger.debug("Generating plot '{}'".format(title))
        fig = plt.figure()
        fig.suptitle(title)
        self._plot(item, fig)
        yield fig

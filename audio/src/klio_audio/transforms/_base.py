# Copyright 2020 Spotify AB

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

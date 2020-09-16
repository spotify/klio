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

import librosa
import librosa.display
import numpy as np

from klio.transforms import decorators as tfm_decorators

from klio_audio import decorators
from klio_audio.transforms import _base


class LoadAudio(_base.KlioAudioBaseDoFn):
    """Load audio into memory as a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.load` takes in a :class:`PCollection
    <apache_beam.pvalue.PCollection>` of :ref:`KlioMessages <klio-message>`
    with the payload of the ``KlioMessage`` a file-like object or a path
    to a file, and returns a ``PCollection`` of ``KlioMessages`` where the
    payload is a :class:`numpy.ndarray`.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                # other transforms
            )

    Args:
        librosa_kwargs (dict): Instantiate the transform with keyword
            arguments to pass into :func:`librosa.load`.
    """

    def __init__(self, *_, **librosa_kwargs):
        self.librosa_kwargs = librosa_kwargs

    @tfm_decorators._handle_klio
    @decorators.handle_binary(save_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Loading {} into memory as a numpy array.".format(element)
        )
        audio, _ = librosa.load(item.payload, **self.librosa_kwargs)
        yield audio


class GetSTFT(_base.KlioAudioBaseDoFn):
    """Calculate Short-time Fourier transform from a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.stft` and expects a :class:`PCollection
    <apache_beam.pvalue.PCollection>` of :ref:`KlioMessages <klio-message>`
    where the payload is a :class:`numpy.ndarray` and the output is the
    same with the ``stft`` calculation applied.

    The Short-time Fourier transform (STFT) is a Fourier-related
    transform used to determine the sinusoidal frequency and phase
    content of local sections of a signal as it changes over time.
    STFT provides the time-localized frequency information for
    situations in which frequency components of a signal vary over time,
    whereas the standard Fourier transform provides the frequency
    information averaged over the entire signal time interval.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetSTFT
                # other transforms
            )

    Args:
        librosa_kwargs (dict): Instantiate the transform with keyword
            arguments to pass into :func:`librosa.stft`.
    """

    def __init__(self, *_, **librosa_kwargs):
        self.librosa_kwargs = librosa_kwargs

    @tfm_decorators._handle_klio
    @decorators.handle_binary(load_with_numpy=True, save_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Calculating the short-time Fourier transform for {}".format(
                element
            )
        )
        yield librosa.stft(y=item.payload, **self.librosa_kwargs)


class GetSpec(_base.KlioAudioBaseDoFn):
    """Generate a dB-scaled spectrogram from a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.amplitude_to_db` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` and the output is the same with the ``amplitude_to_
    db`` function applied.

    A spectrogram shows the the intensity of frequencies over time.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetSpec()
                # other transforms
            )

    Args:
        librosa_kwargs (dict): Instantiate the transform with keyword
            arguments to pass into :func:`librosa.amplitude_to_db`.
    """

    def __init__(self, *_, **librosa_kwargs):
        self.librosa_kwargs = librosa_kwargs

    @tfm_decorators._handle_klio
    @decorators.handle_binary(load_with_numpy=True, save_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Generating a spectrogram for {}".format(element)
        )
        stft = item.payload
        yield librosa.amplitude_to_db(
            np.abs(stft), ref=np.max(np.abs(stft)), **self.librosa_kwargs
        )


class GetMelSpec(_base.KlioAudioBaseDoFn):
    """Generate a spectrogram from a :class:`numpy.ndarray` using the mel scale.

    This transform wraps :func:`librosa.feature.melspectrogram` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` and the output is the same with the
    ``melspectrogram`` function applied.

    The mel scale is a non-linear transformation of frequency scale
    based on the perception of pitches. The mel scale is calculated so
    that two pairs of frequencies separated by a delta in the mel scale
    are perceived by humans as being equidistant.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetMelSpec()
                # other transforms
            )

    Args:
        librosa_kwargs (dict): Instantiate the transform with keyword
            arguments to pass into :func:`librosa.feature.melspectrogram`.
    """

    def __init__(self, *_, **librosa_kwargs):
        self.librosa_kwargs = librosa_kwargs

    @tfm_decorators._handle_klio
    @decorators.handle_binary(load_with_numpy=True, save_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Generating a Mel spectrogram for {}".format(element)
        )
        yield librosa.feature.melspectrogram(
            y=item.payload, **self.librosa_kwargs
        )


class GetMFCC(_base.KlioAudioBaseDoFn):
    """Calculate MFCCs from a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.power_to_db` followed by
    :func:`librosa.feature.mfcc` and expects a :class:`PCollection
    <apache_beam.pvalue.PCollection>` of :ref:`KlioMessages <klio-message>`
    where the payload is a :class:`numpy.ndarray` and the output is the same
    with the ``mfcc`` function applied.

    The Mel frequency cepstral coefficients (MFCCs) of a signal are
    a small set of features (usually about 10–20) which describe the
    overall shape of a spectral envelope. It's is often used to describe
    timbre or model characteristics of human voice.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetMFCC()
                # other transforms
            )

    Args:
        librosa_kwargs (dict): Instantiate the transform with keyword
            arguments to pass into :func:`librosa.feature.mfcc`.
    """

    def __init__(self, *_, **librosa_kwargs):
        self.librosa_kwargs = librosa_kwargs

    @tfm_decorators._handle_klio
    @decorators.handle_binary(load_with_numpy=True, save_with_numpy=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        self._klio.logger.debug(
            "Generating Mel frequency cepstral coefficients for {}".format(
                element
            )
        )
        # melspectrogram by default returns a power'ed (**2) spectrogram
        # so we need to convert to decibel units (if it wasn't a power'ed
        # spec, then we'd use amplitude_to_db)
        Sdb = librosa.power_to_db(item.payload, ref=np.max)
        yield librosa.feature.mfcc(S=Sdb, **self.librosa_kwargs)


class SpecToPlot(_base.KlioPlotBaseDoFn):
    """Generate a matplotlib figure of the spectrogram of a
    :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.display.specshow` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` of a spectrogram and the output is a
    :class:`matplotlib.figure.Figure` instance.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetSpec()
                | audio.SpecToPlot()
                # other transforms
            )

    Args:
        title (str): Title of spectrogram plot. Default: ``Spectrogram of
            {KlioMessage.data.element}``.
        plot_args (dict): keyword arguments to pass to
            :func:`librosa.display.specshow`.
    """

    DEFAULT_TITLE = "Spectrogram of {element}"

    def __init__(self, *_, title=None, **plot_args):
        super(SpecToPlot, self).__init__(self, title=title, **plot_args)
        self.plot_args["x_axis"] = self.plot_args.get("x_axis", "time")
        self.plot_args["y_axis"] = self.plot_args.get("y_axis", "linear")

    def _plot(self, item, fig):
        librosa.display.specshow(item.payload, ax=fig.gca(), **self.plot_args)


class MelSpecToPlot(_base.KlioPlotBaseDoFn):
    """Generate a matplotlib figure of the mel spectrogram of a
    a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.power_to_db` followed by
    :func:`librosa.display.specshow` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` of a melspectrogram and the output is a
    :class:`matplotlib.figure.Figure` instance.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetMelSpec()
                | audio.SpecToPlot()
                # other transforms
            )

    Args:
        title (str): Title of spectrogram plot. Default: ``Mel-freqency
            Spectrogram of {KlioMessage.data.element}``.
        plot_args (dict): keyword arguments to pass to
            :func:`librosa.display.specshow`.
    """

    DEFAULT_TITLE = "Mel-frequency Spectrogram of {element}"

    def __init__(self, *_, title=None, **plot_args):
        super(MelSpecToPlot, self).__init__(self, title=title, **plot_args)
        self.plot_args["y_axis"] = "mel"
        self.plot_args["x_axis"] = self.plot_args.get("x_axis", "time")
        self.plot_args["fmax"] = self.plot_args.get("fmax", 8000)

    def _plot(self, item, fig):
        Sdb = librosa.power_to_db(item.payload, ref=np.max)
        librosa.display.specshow(Sdb, ax=fig.gca(), **self.plot_args)


class MFCCToPlot(_base.KlioPlotBaseDoFn):
    """Generate a matplotlib figure of the MFCCs as a :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.display.specshow` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` of the MFCCs of an audio and the output is a
    :class:`matplotlib.figure.Figure` instance.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.GetMFCC()
                | audio.MFCCToPlot()
                # other transforms
            )

    Args:
        title (str): Title of spectrogram plot. Default: ``MFCCs of
            {KlioMessage.data.element}``.
        plot_args (dict): keyword arguments to pass to
            :func:`librosa.display.specshow`.
    """

    DEFAULT_TITLE = "MFCCs of {element}"

    def __init__(self, *_, title=None, **plot_args):
        super(MFCCToPlot, self).__init__(self, title=title, **plot_args)
        self.plot_args["x_axis"] = self.plot_args.get("x_axis", "time")

    def _plot(self, item, fig):
        librosa.display.specshow(item.payload, ax=fig.gca(), **self.plot_args)


class WaveformToPlot(_base.KlioAudioBaseDoFn):
    """Generate a matplotlib figure of the wave form of a
    :class:`numpy.ndarray`.

    This transform wraps :func:`librosa.display.waveplot` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`numpy.ndarray` of a loaded audio file the output is a
    :class:`matplotlib.figure.Figure` instance.

    Example:

    .. code-block:: python

        # run.py
        import apache_beam as beam
        from klio.transforms import decorators
        from klio_audio.transforms import audio

        @decorators.handle_klio
        def element_to_filename(ctx, data):
            filename = data.element.decode("utf-8")
            return f"file:///path/to/audio/{filename}.wav"

        def run(in_pcol, job_config):
            return (
                in_pcol
                | beam.Map(element_to_filename)
                | audio.LoadAudio()
                | audio.WaveformToPlot()
                # other transforms
            )

    Args:
        num_samples (int): Number of samples to plot. Default: ``5000``.
        title (str): Title of spectrogram plot. Default: ``Waveplot of
            {KlioMessage.data.element}``.
        plot_args (dict): keyword arguments to pass to
            :func:`librosa.display.waveplot`.
    """

    DEFAULT_TITLE = "Waveplot of {element}"

    def __init__(self, *_, num_samples=5000, title=None, **plot_args):
        super(WaveformToPlot, self).__init__(self, title=title, **plot_args)
        self.num_samples = num_samples

    def _plot(self, item, fig):
        librosa.display.waveplot(
            item.payload[: self.num_samples], ax=fig.gca(), **self.plot_args
        )

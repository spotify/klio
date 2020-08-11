# Copyright 2020 Spotify AB

import librosa
import librosa.display
import numpy as np

from klio.transforms import decorators as tfm_decorators

from klio_audio import decorators
from klio_audio.transforms import _base


class LoadAudio(_base.KlioAudioBaseDoFn):
    """Load audio into memory as a numpy array.

    Input should be a file-like object or a path to a file.

    Returns a loaded audio file as a numpy array (a floating point
    time series).
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
    """Calculate Short-time Fourier transform from audio.

    The Short-time Fourier transform (STFT) is a Fourier-related
    transform used to determine the sinusoidal frequency and phase
    content of local sections of a signal as it changes over time.
    STFT provides the time-localized frequency information for
    situations in which frequency components of a signal vary over time,
    whereas the standard Fourier transform provides the frequency
    information averaged over the entire signal time interval.

    Input should be a loaded audio file as a numpy array (i.e. the
    output of the `LoadAudio` transform).

    Returns an numpy array of the complex-valued matrix of short-term
    Fourier transform coefficients.
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
    """Generate a spectrogram from audio.

    A spectrogram shows the the intensity of frequencies over time, and
    is just the squared magnitude of the STFT.

    Input should be the STFT of an audio file as a numpy array (i.e. the
    output of the `GetSTFT` transform).

    Returns an numpy array of the computed spectrogram of the given audio.
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
    """Generate a spectrogram from audio using the mel scale.

    The mel scale is a non-linear transformation of frequency scale
    based on the perception of pitches. The mel scale is calculated so
    that two pairs of frequencies separated by a delta in the mel scale
    are perceived by humans as being equidistant.

    Input should be a loaded audio file as a numpy array (i.e. the
    output of the `LoadAudio` transform).

    Returns an numpy array of the computed mel spectrogram of the given
    audio.
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
    """Calculate MFCCs from audio.

    The Mel frequency cepstral coefficients (MFCCs) of a signal are
    a small set of features (usually about 10–20) which describe the
    overall shape of a spectral envelope. It's is often used to describe
    timbre or model characteristics of human voice.

    Input should be a loaded audio file as a numpy array (i.e. the
    output of the `LoadAudio` transform).

    Returns an numpy array of the computed MFCC sequence of the given
    audio.
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
    """Generate a matplotlib figure of the spectrogram of audio.

    Input should be a generated spectrogram as a numpy array (i.e. the
    output of the `GetSpec` transform).

    Returns a matplotlib.figure.Figure of the mel spectrogram of the given
    audio.
    """

    DEFAULT_TITLE = "Spectrogram of {element}"

    def __init__(self, *_, title=None, **plot_args):
        super(SpecToPlot, self).__init__(self, title=title, **plot_args)
        self.plot_args["x_axis"] = self.plot_args.get("x_axis", "time")
        self.plot_args["y_axis"] = self.plot_args.get("y_axis", "linear")

    def _plot(self, item, fig):
        librosa.display.specshow(item.payload, ax=fig.gca(), **self.plot_args)


class MelSpecToPlot(_base.KlioPlotBaseDoFn):
    """Generate a matplotlib figure of the mel spectrogram of audio.

    Input should be a generated mel spectrogram as a numpy array (i.e. the
    output of the `GetMelSpec` transform).

    Returns a matplotlib.figure.Figure of the mel spectrogram of the given
    audio.
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
    """Generate a matplotlib figure of the MFCCs of audio.

    Input should be the MFCCs of an audio as a numpy array (i.e. the
    output of the `GetMFCC` transform).

    Returns a matplotlib.figure.Figure of the MFCC sequence of the given
    audio.
    """

    DEFAULT_TITLE = "MFCCs of {element}"

    def __init__(self, *_, title=None, **plot_args):
        super(MFCCToPlot, self).__init__(self, title=title, **plot_args)
        self.plot_args["x_axis"] = self.plot_args.get("x_axis", "time")

    def _plot(self, item, fig):
        librosa.display.specshow(item.payload, ax=fig.gca(), **self.plot_args)


class WaveformToPlot(_base.KlioAudioBaseDoFn):
    """Generate a matplotlib figure of the wave form of audio.

    Input should be a loaded audio file as a numpy array (i.e. the
    output of the `LoadAudio` transform).

    Returns a matplotlib.figure.Figure of the wave form of the given
    audio.
    """

    DEFAULT_TITLE = "Waveplot of {element}"

    def __init__(self, *_, num_samples=5000, title=None, **plot_args):
        super(WaveformToPlot, self).__init__(self, title=title, **plot_args)
        self.num_samples = num_samples

    def _plot(self, item, fig):
        librosa.display.waveplot(
            item.payload[: self.num_samples], ax=fig.gca(), **self.plot_args
        )

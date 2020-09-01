# Copyright 2020 Spotify AB

import logging
import warnings

import apache_beam as beam

from klio_audio.transforms import io as aio
from klio_audio.transforms import audio

import transforms


warnings.simplefilter("ignore")
loggers_to_mute = (
    "apache_beam.io.filebasedsink",
    "apache_beam.runners.worker.statecache",
    "apache_beam.runners.portability.fn_api_runner",
    "apache_beam.runners.portability.fn_api_runner_transforms",
    "apache_beam.internal.gcp.auth",
    "oauth2client.transport",
    "oauth2client.client",
)
for logger in loggers_to_mute:
    logging.getLogger(logger).setLevel(logging.ERROR)
logging.getLogger("klio").setLevel(logging.DEBUG)


def run(in_pcol, job_config):
    # load 5 seconds of audio and get STFT
    stft = (
        in_pcol
        | aio.GcsLoadBinary()
        | audio.LoadAudio(offset=10, duration=5)
        | audio.GetSTFT()
    )

    # get magnitude of audio
    magnitude = (
        stft | "Get magnitude" >> beam.ParDo(transforms.GetMagnitude()).with_outputs()
    )

    # map the result to a key (the KlioMessage element)
    # so we can group all results by key
    magnitude_key = (
        magnitude.spectrogram
        | "element to spec" >> beam.Map(transforms.create_key_from_element)
    )
    # get nearest neighbors and map the result to a key (the KlioMessage element)
    nn_filter = (
        magnitude.spectrogram
        | "Get nn filter" >> beam.ParDo(transforms.FilterNearestNeighbors())
        | "element to filter" >> beam.Map(transforms.create_key_from_element)
    )

    # map together the full magnitude with its filter by key  (the KlioMessage element)
    merge = (
        {"full": magnitude_key, "nnfilter": nn_filter}
        | "merge" >> beam.CoGroupByKey()
    )

    # calc the difference between full magnitude and the filter
    net = merge | beam.Map(transforms.subtract_filter_from_full)

    # create a mask from the filter minus the difference of full & filter
    first_mask = (
        {"first": nn_filter, "second": net, "full": magnitude_key}
        | "first mask group" >> beam.CoGroupByKey()
        | "first mask" >> beam.ParDo(transforms.GetSoftMask(margin=2))
    )
    # create another mask from the difference of full & filter minus the filter
    second_mask = (
        {"first": net, "second": nn_filter, "full": magnitude_key}
        | "second mask group" >> beam.CoGroupByKey()
        | "second mask" >> beam.ParDo(transforms.GetSoftMask(margin=10))
    )

    # plot the full magnitude spectrogram
    magnitude_out = (
        magnitude.spectrogram
        | "full spec" >> audio.GetSpec()
        | "plot full spec" >> audio.SpecToPlot(title="Full Spectrogam for {element}", y_axis="log")
        | "save full" >> aio.GcsUploadPlot(suffix="-full")
    )
    # plot the first mask (background) spectrogram
    background_out = (
        first_mask
        | "background spec" >> audio.GetSpec()
        | "plot background spec" >> audio.SpecToPlot(title="Background Spectrogam for {element}", y_axis="log")
        | "save background" >> aio.GcsUploadPlot(suffix="-background")
    )
    # plot the second mask (foreground) spectrogram
    foreground_out = (
        second_mask
        | "foreground spec" >> audio.GetSpec()
        | "plot forground spec" >> audio.SpecToPlot(title="Foreground Spectrogam for {element}", y_axis="log")
        | "save foreground" >> aio.GcsUploadPlot(suffix="-foreground")
    )

    return (
        (magnitude_out, background_out, foreground_out)
        | "flatten output paths" >> beam.Flatten()
        | "remove dups" >> beam.Distinct()
    )

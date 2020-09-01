# Copyright 2020 Spotify AB

import logging

import apache_beam as beam

import transforms


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


def run(input_pcol, config):
    return input_pcol | beam.ParDo(transforms.LogKlioMessage())

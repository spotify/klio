# Copyright 2020 Spotify AB

import logging
import warnings

import apache_beam as beam
from apache_beam.io import textio


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
logging.getLogger("klio").setLevel(logging.INFO)


def logme(item, config):
    logging.info(f"Received item: {item}")
    logging.info(f"Reeived config: {config}")
    return item


def run(pipeline, config):
    data_input = config.job_config.data.inputs[0]
    data_output = config.job_config.data.outputs[0]

    out = (
        pipeline
        | textio.ReadFromText(config.job_config.events.inputs[0].my_custom_event_input_key)
        | "data input" >> beam.Map(lambda x: logme(x, data_input))
        | "data output" >> beam.Map(lambda x: logme(x, data_output))
        | textio.WriteToText(config.job_config.events.outputs[0].my_custom_event_output_key)

    )
    return out

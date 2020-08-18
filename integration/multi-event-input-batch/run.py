# Copyright 2020 Spotify AB

import logging
import warnings

import apache_beam as beam

from klio.transforms import decorators


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


@decorators.handle_klio
def first_func(ctx, item):
    ctx.logger.info(f"[first_func]: {item.element}")
    return item


@decorators.handle_klio
def second_func(ctx, item):
    ctx.logger.info(f"[second_func]: {item.element}")
    return item


@decorators.handle_klio
def combined_func(ctx, item):
    ctx.logger.info(f"[combined_func]: {item.element}")
    return item


def run(pcolls, config):
    first = pcolls.file0 | "process first" >> beam.Map(first_func)
    second = pcolls.file1 | "process second" >> beam.Map(second_func)
    combined = (first, second) | beam.Flatten()
    return combined | "process combined" >> beam.Map(combined_func)


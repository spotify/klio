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
    # The concurrency logs may be different for every machine, so let's
    # just turn them off
    "klio.concurrency",
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


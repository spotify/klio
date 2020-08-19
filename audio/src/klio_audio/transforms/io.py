# Copyright 2020 Spotify AB

import io
import os

from apache_beam.io.gcp import gcsio

from klio.transforms import decorators as tfm_decorators

from klio_audio import decorators
from klio_audio.transforms import _base


# TODO: handle multiple data inputs
class GcsLoadBinary(_base.KlioAudioBaseDoFn):
    """Download binary file from GCS into memory.

    This transform uses the job_config.data.inputs configuration in
    a job's `klio-job.yaml` file.

    Returns a file-like bytes object of the downloaded audio.
    """

    def setup(self):
        self.client = gcsio.GcsIO()

    @tfm_decorators._handle_klio
    @decorators.handle_binary(skip_load=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        input_data = self._klio.config.job_config.data.inputs[0]
        file_suffix = input_data.file_suffix
        if not file_suffix.startswith("."):
            file_suffix = "." + file_suffix
        filename = element + file_suffix
        input_path = os.path.join(input_data.location, filename)

        self._klio.logger.debug(
            "Downloading {} from {}".format(filename, input_data.location)
        )
        with self.client.open(input_path, "rb") as source:
            out = io.BytesIO(source.read())
        self._klio.logger.debug("Downloaded {}".format(filename))
        yield out


# TODO: handle multiple data outputs
class GcsUploadPlot(_base.KlioAudioBaseDoFn):
    """Upload matplotlib figure to GCS.

    This transform uses the job_config.data.outputs configuration in
    a job's `klio-job.yaml` file.

    Input should be a matplotlib.figure.Figure (i.e. the output of any
    of the *Plot transforms in the klio_audio.transforms.audio module).

    Returns the uploaded location as a bytestring.
    """

    def __init__(self, prefix="", suffix="", file_format=None, **plt_kwargs):
        self.prefix = prefix
        self.suffix = suffix
        self.file_format = file_format
        self.plt_kwargs = plt_kwargs

    def setup(self):
        self.client = gcsio.GcsIO()

    @tfm_decorators._handle_klio
    @decorators.handle_binary(skip_dump=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        output_data = self._klio.config.job_config.data.outputs[0]
        file_suffix = output_data.file_suffix
        if not file_suffix.startswith("."):
            file_suffix = "." + file_suffix
        filename = self.prefix + element + self.suffix + file_suffix
        output_path = os.path.join(output_data.location, filename)

        source = io.BytesIO()
        fig = item.payload
        fig_format = self.file_format or file_suffix.lstrip(".")
        self._klio.logger.debug(
            "Saving plot as {} for {}".format(fig_format, element)
        )
        fig.savefig(source, format=fig_format, **self.plt_kwargs)

        with self.client.open(output_path, "wb") as out:
            out.write(source.getvalue())

        self._klio.logger.debug("Saved plot to {}".format(output_path))
        yield output_path

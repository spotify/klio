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

    This transform uses the ``job_config.data.inputs`` configuration in
    a job's ``klio-job.yaml`` file.

    This transform uses Apache's native :class:`GCS client
    <apache_beam.io.gcp.gcsio.GcsIO>` and expects a
    :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>`, and  returns with a payload of the
    downloaded binary as a file-like bytes object.

    Example:

    .. code-block:: python

        # run.py
        from klio_audio.transforms import audio
        from klio_audio.transforms import io

        def run(in_pcol, job_config):
            return (
                in_pcol
                | io.GcsLoadBinary()
                | audio.LoadAudio()
                # other transforms
            )

    .. code-block:: yaml

        # klio-job.yaml
        # <-- snip -->
        job_config:
          events:
            # <-- snip -->
          data:
            inputs:
              - type: file
                location: gs://my-bucket/input
                file_suffix: .ogg
              # <-- snip -->
    """

    def setup(self):
        self.client = gcsio.GcsIO()

    @tfm_decorators._handle_klio
    @decorators.handle_binary(skip_load=True)
    def process(self, item):
        element = item.element.decode("utf-8")
        input_data_config = self._klio.config.job_config.data.inputs

        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        if len(input_data_config) == 0:
            raise RuntimeError(
                "The `klio_audio.transforms.io.GcsLoadBinary` transform "
                "requires a data input to be configured in "
                "`klio-job.yaml::job_config.data.inputs`."
            )

        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        if len(input_data_config) > 1:
            raise RuntimeError(
                "The `klio_audio.transforms.io.GcsLoadBinary` transform "
                "does not support multiple configured inputs in "
                "`klio-job.yaml::job_config.data.inputs`."
            )
        input_data = input_data_config[0]

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
    """Upload a matplotlib :class:`figure <matplotlib.figure.Figure>` to GCS.

    This transform uses the ``job_config.data.outputs`` configuration in
    a job's ``klio-job.yaml`` file.

    This transform wraps :class:`savefig <matplotlib.figure.Figure>` and
    expects a :class:`PCollection <apache_beam.pvalue.PCollection>` of
    :ref:`KlioMessages <klio-message>` where the payload is a
    :class:`matplotlib.figure.Figure` and returns with a payload of the
    uploaded file location as ``bytes``.

    Example:

    .. code-block:: python

        # run.py
        from klio_audio.transforms import audio
        from klio_audio.transforms import io

        def run(in_pcol, job_config):
            return (
                in_pcol
                | io.GcsLoadBinary()
                | audio.LoadAudio()
                | audio.GetSpec()
                | audio.SpecToPlot()
                | io.GcsUploadPlot()
            )

    .. code-block:: yaml

        # klio-job.yaml
        # <-- snip -->
        job_config:
          # <-- snip -->
          data:
            inputs:
              # <-- snip -->
            outputs:
              - type: file
                location: gs://my-bucket/output
                file_suffix: .png

    Args:
        prefix (str): filename prefix. Default: ``""``
        suffix (str): filename suffix. Default: ``""``
        file_format (str): plot format (e.g. png). Defaults to the file
            suffix as configured in
            ``klio-job.yaml::job_config.data.outputs[].file_suffix``.
        plt_kwargs (dict): keyword arguments to pass to
            :class:`savefig <matplotlib.figure.Figure>`.
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
        output_data_config = self._klio.config.job_config.data.outputs

        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        if len(output_data_config) == 0:
            raise RuntimeError(
                "The `klio_audio.transforms.io.GcsUploadPlot` transform "
                "requires a data output to be configured in "
                "`klio-job.yaml::job_config.data.outputs`."
            )

        # raise a runtime error so it actually crashes klio/beam rather than
        # just continue processing elements
        if len(output_data_config) > 1:
            raise RuntimeError(
                "The `klio_audio.transforms.io.GcsUploadPlot` transform "
                "does not support multiple configured outputs in "
                "`klio-job.yaml::job_config.data.outputs`."
            )

        output_data = output_data_config[0]

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

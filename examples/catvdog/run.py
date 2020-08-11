# -*- coding: utf-8 -*-
# Copyright 2019 Spotify AB

import apache_beam as beam

import transforms


def run(input_pcol, config):
    """Main entrypoint in running a job's transform(s).

    Run any Beam transforms that need to happen after a message is
    consumed from PubSub from an upstream job (if not an apex job),
    and before  publishing a message to any downstream job (if
    needed/configured).

    Args:
        input_pcol: A Beam PCollection returned from
            ``beam.io.ReadFromPubSub``.
        config (klio.KlioConfig): Job-related configuration as
            defined in ``klio-job.yaml``.
    Returns:
        A Beam PCollection that will be passed to ``beam.io.WriteToPubSub``.
    """
    output_pcol = input_pcol | beam.ParDo(transforms.CatVDog())
    return output_pcol

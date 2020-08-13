# Copyright 2020 Spotify AB

import apache_beam as beam

import transforms

def run(input_pcol, config):
    return input_pcol | beam.ParDo(transforms.LogKlioMessage())

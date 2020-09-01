# Copyright 2020 Spotify AB

import apache_beam as beam

from klio.transforms import decorators


class LogKlioMessage(beam.DoFn):
    @decorators.handle_klio
    def process(self, item):
        self._klio.logger.info("Received element {}".format(item.element))
        self._klio.logger.info("Received payload {}".format(item.payload))
        yield item

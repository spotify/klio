# Copyright 2020 Spotify AB
"""
Klio DoFn for basic integration test.
"""

from klio import transforms


class LogKlioMessage(transforms.KlioBaseDoFn):
    def process(self, item):
        self._klio.logger.info("Hello, Klio!")
        self._klio.logger.info("Received element {}".format(item.element))
        self._klio.logger.info("Received payload {}".format(item.payload))
        yield item

    def input_data_exists(self, item):
        return True

    def output_data_exists(self, item):
        return False

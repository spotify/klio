"""
Notice: the code below is just an example of what can be done.

Feel free to import what's needed, including third-party libraries or
other self-written modules.
"""

import apache_beam as beam

from klio.transforms import decorators

class HelloKlio(beam.DoFn):
    @decorators.handle_klio
    def process(self, item):
        self._klio.logger.info("Hello, Klio!")
        self._klio.logger.info("Received element {}".format(item.element))
        self._klio.logger.info("Received payload {}".format(item.payload))
        yield item

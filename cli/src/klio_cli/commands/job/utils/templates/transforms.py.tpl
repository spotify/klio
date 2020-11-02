"""
Notice: the code below is just an example of what can be done.

Feel free to import what's needed, including third-party libraries or
other self-written modules.
"""
import apache_beam as beam

from klio.transforms import decorators


class HelloKlio(beam.DoFn):
    """A simple DoFn."""

    @decorators.handle_klio
    def process(self, data):
        """Main entrypoint to a transform.

        Any errors raised here (explicitly or not), Klio will catch and
        log + drop the Klio message.

        For information on the Klio message, see
        https://docs.klio.io/en/latest/userguide/pipeline/message.html

        For information on yielding other information other than ``data``, see
        https://docs.klio.io/en/latest/userguide/pipeline/state.html

        Args:
            data (KlioMessage.data): The data of the Klio message, which
                contains two attributes: ``data.element`` and
                ``data.payload``.
        Yields:
            KlioMessage.data: the same Klio message data object received.
        """
        element = data.element.decode("utf-8")
        self._klio.logger.info(
            "Received '%s' from Pub/Sub topic '%s'"
            % (element, self._klio.config.job_config.events.inputs[0].topic)
        )
        yield data


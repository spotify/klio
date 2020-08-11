"""
Notice: the code below is just an example of what can be done.

Feel free to import what's needed, including third-party libraries or
other self-written modules.
"""

from klio import transforms


class HelloKlio(transforms.KlioBaseDoFn):
    """A simple DoFn."""

    def process(self, entity_id):
        """Main entrypoint to a transform.

        Any errors raised here (explicitly or not), Klio will catch and
        log + drop the entity_id/message.

        Args:
            entity_id (str): the referential identifier that points to
                an audio file on which the job will perform work.
        Returns:
            entity_id (str)
        """
        self._klio.logger.info(
            "Received '%s' from Pub/Sub topic '%s'"
            % (entity_id, self._klio.config.job_config.inputs[0].topic)
        )
        return entity_id

    def input_data_exists(self, entity_id):
        """Check if input data for given entity ID exists.

        If this klio job has no dependencies on parent jobs (aka this is
        an "apex" node), simply return `True`.

        Otherwise, implement logic needed to check if the parent job(s)
        output data is available for input into this klio job.

        Args:
            entity_id (str): a referential identifier that points to an
                audio file found in the configured input data location.

        Returns:
            (bool) whether or not the expected input data exists for the
                given entity_id.
        """
        return True

    def output_data_exists(self, entity_id):
        """Check if the supposed output data for a given entity ID exists.

        If this klio job should always create output, simply return
        `True`.

        Otherwise, implement logic needed to check if output data exists
        to avoid duplicate work.

        Args:
            entity_id (str): a referential identifier that points to an
                audio file found in the configured output data location.

        Returns:
            (bool) whether or not the output data exists for the given
                entity_id.
        """
        return False

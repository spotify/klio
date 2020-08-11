# Copyright 2020 Spotify AB

import apache_beam as beam

from apache_beam import pvalue

from klio.transforms import _helpers
from klio.transforms import decorators
from klio.transforms import io as io_transforms


class KlioGcsCheckInputExists(
    _helpers._KlioInputDataMixin, _helpers._KlioGcsCheckExistsBase
):
    """Klio transform to check input exists in GCS."""

    pass


class KlioGcsCheckOutputExists(
    _helpers._KlioOutputDataMixin, _helpers._KlioGcsCheckExistsBase
):
    """Klio transform to check output exists in GCS."""

    pass


class KlioFilterPing(
    _helpers._KlioInputDataMixin, _helpers._KlioBaseDataExistenceCheck
):
    """Klio transform to tag outputs if in ping mode or not."""

    def ping(self, kmsg):
        global_ping = self._data_config.ping
        msg_ping = kmsg.metadata.ping
        return msg_ping if msg_ping else global_ping

    def process(self, kmsg):
        tagged_state = _helpers.TaggedStates.DEFAULT
        item = kmsg.data.v2.element.decode("utf-8")

        if self.ping(kmsg):
            self._klio.logger.info("Pass through '%s': Ping mode ON." % item)
            tagged_state = _helpers.TaggedStates.PASS_THRU

        else:
            self._klio.logger.debug("Process '%s': Ping mode OFF." % item)
            tagged_state = _helpers.TaggedStates.PROCESS

        yield pvalue.TaggedOutput(tagged_state.value, kmsg.SerializeToString())


class KlioFilterForce(
    _helpers._KlioOutputDataMixin, _helpers._KlioBaseDataExistenceCheck
):
    """Klio transform to tag outputs if in force mode or not."""

    def force(self, kmsg):
        global_force = self._data_config.force
        msg_force = kmsg.metadata.force
        return msg_force if msg_force else global_force

    def process(self, kmsg):
        tagged_state = _helpers.TaggedStates.DEFAULT
        item_path = self._get_absolute_path(kmsg.data.v2.element)
        item = kmsg.data.v2.element.decode("utf-8")

        if not self.force(kmsg):
            self._klio.logger.info(
                "Pass through '%s': Force mode OFF with output found at '%s'."
                % (item, item_path)
            )
            tagged_state = _helpers.TaggedStates.PASS_THRU

        else:
            self._klio.logger.info(
                "Process '%s': Force mode ON with output found at '%s'."
                % (item, item_path)
            )
            tagged_state = _helpers.TaggedStates.PROCESS

        yield pvalue.TaggedOutput(tagged_state.value, kmsg.SerializeToString())


# TODO: define a helper transform that can trigger parents of a streaming job.
class KlioWriteToEventOutput(beam.PTransform):
    """Klio composite transform to write to the configured event output."""

    # NOTE: hopefully we don't get an dict lookup errors since KlioConfig
    # should raise if given an unsupported event IO transform
    CONFNAME_TO_OUT_TRANSFORM = {
        "file": io_transforms.KlioWriteToText,
        "pubsub": beam.io.WriteToPubSub,
    }

    @property
    @decorators._set_klio_context
    def _event_config(self):
        # TODO: figure out how to support multiple outputs
        return self._klio.config.job_config.event_outputs[0]

    def expand(self, pcoll):
        transform_kls = self.CONFNAME_TO_OUT_TRANSFORM[self._event_config.name]

        kwargs = self._event_config.as_dict()
        return (
            pcoll
            | "Writing to '%s'" % self._event_config.name
            >> transform_kls(**kwargs)
        )


# NOTE: not doing much right now, but allows us to extend if need be
class KlioDrop(beam.DoFn, metaclass=_helpers._KlioBaseDoFnMetaclass):
    """Klio DoFn to log & drop a KlioMessage."""

    WITH_OUTPUTS = False

    @decorators._handle_klio
    def process(self, kmsg):
        self._klio.logger.info(
            "Dropping KlioMessage - can not process '%s' any further."
            % kmsg.element
        )
        return

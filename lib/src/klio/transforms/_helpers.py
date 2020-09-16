# Copyright 2020 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import enum
import functools
import os

import apache_beam as beam

from apache_beam import pvalue
from apache_beam.io.gcp import gcsio

from klio.message import serializer
from klio.transforms import _utils
from klio.transforms import core


class DataExistState(enum.Enum):
    FOUND = "found"
    NOT_FOUND = "not_found"

    # to human-friendly strings for easy logging
    @classmethod
    def to_str(cls, attr):
        if attr == cls.NOT_FOUND:
            return "not found"
        return "found"


# TODO mabe use klio core config's defs but make them strings
class KlioIODirection(enum.Enum):
    INPUT = "input"
    OUTPUT = "output"


# TODO: maybe use common.MessageState which then should be mapped to
# strings instead of ints (or have a method that does the conversion)
class TaggedStates(enum.Enum):
    PROCESS = "process"
    PASS_THRU = "pass_thru"
    DROP = "drop"
    DEFAULT = "tag_not_set"


# Only serializes to a KlioMessage; we deserialize within the process
# method itself since we also have to tag the output (too difficult to
# serialize output that's already tagged)
def _wrap_process(meth):
    @functools.wraps(meth)
    def wrapper(self, incoming_item, *args, **kwargs):
        try:
            kmsg = serializer.to_klio_message(
                incoming_item, self._klio.config, self._klio.logger
            )
            yield from meth(self, kmsg, *args, **kwargs)

        except Exception as err:
            self._klio.logger.error(
                "Dropping KlioMessage - exception occurred when serializing "
                "'%s' to a KlioMessage.\nError: %s" % (incoming_item, err),
                exc_info=True,
            )
            return

    return wrapper


def _job_in_jobs(current_job, job_list):
    # Use job name & project to ensure uniqueness
    curr_job_name = "{}-{}".format(
        current_job.gcp_project, current_job.job_name
    )
    downstream_job_names = [
        "{}-{}".format(j.gcp_project, j.job_name) for j in job_list
    ]

    return curr_job_name in downstream_job_names


class _KlioBaseDoFnMetaclass(type):
    """Enforce behavior upon subclasses of `_KlioBaseDataExistenceCheck`."""

    def __init__(cls, name, bases, clsdict):
        if not getattr(cls, "_klio", None):
            setattr(cls, "_klio", core.KlioContext())

        if os.getenv("KLIO_TEST_MODE", "").lower() in ("true", "1"):
            return

        # TODO: fixme: not every child class will inherit from
        # _KlioBaseDataExistenceCheck
        if _utils.is_original_process_func(
            clsdict, bases, base_class="_KlioBaseDataExistenceCheck"
        ):

            setattr(cls, "process", _wrap_process(clsdict["process"]))

            cls._klio._transform_name = name

    def __call__(self, *args, **kwargs):
        # automatically wrap DoFn in a beam.ParDo (with or without
        # `with_outputs` for tagged outputs) so folks can just do
        # `pcoll | KlioInputDataExistenceCheck()` rather than
        # `pcoll | beam.ParDo(KlioInputDataExistenceCheck()).with_outputs()`
        if self.WITH_OUTPUTS is True:
            return beam.ParDo(
                super(_KlioBaseDoFnMetaclass, self).__call__(*args, **kwargs)
            ).with_outputs()

        return beam.ParDo(
            super(_KlioBaseDoFnMetaclass, self).__call__(*args, **kwargs)
        )


class _KlioBaseDataExistenceCheck(beam.DoFn, metaclass=_KlioBaseDoFnMetaclass):
    """Base class for data existence checking."""

    DIRECTION_PFX = None  # i.e. KlioIODirection.INPUT
    WITH_OUTPUTS = True

    @property
    def _location(self):
        return self._data_config.location

    @property
    def _suffix(self):
        return self._data_config.file_suffix

    @property
    def _data_config(self):
        pass

    def exists(self, *args, **kwargs):
        pass

    def _get_absolute_path(self, element):
        return os.path.join(
            self._location, element.decode("utf-8") + self._suffix
        )


class _KlioInputDataMixin(object):
    """Mixin to add input-specific logic for a data existence check.

    Must be used with _KlioGcsCheckExistsBase
    """

    DIRECTION_PFX = KlioIODirection.INPUT

    @property
    def _data_config(self):
        # TODO: figure out how to support multiple inputs

        # If folks use the default existence checks that klio does, we
        # shouldn't get here. But we could if the user implements their
        # own and misconfigures their data inputs.
        if len(self._klio.config.job_config.data.inputs) > 1:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "Multiple inputs configured in "
                "`klio-job.yaml::job_config.data.inputs` are not supported "
                "for data existence checks."
            )
        # If folks use the default existence checks that klio does, we
        # shouldn't get here. But we could if the user implements their
        # own and misconfigures their data inputs.
        if len(self._klio.config.job_config.data.inputs) == 0:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "Input data existence checks require input data to be "
                "configured in `klio-job.yaml::job_config.data.inputs`."
            )
        return self._klio.config.job_config.data.inputs[0]


class _KlioOutputDataMixin(object):
    """Mixin to add output-specific logic for a data existence check.

    Must be used with _KlioGcsCheckExistsBase
    """

    DIRECTION_PFX = KlioIODirection.OUTPUT

    @property
    def _data_config(self):
        # TODO: figure out how to support multiple outputs

        # If folks use the default existence checks that klio does, we
        # shouldn't get here. But we could if the user implements their
        # own and misconfigures their data outputs.
        if len(self._klio.config.job_config.data.outputs) > 1:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "Multiple outputs configured in "
                "`klio-job.yaml::job_config.data.outputs` are not supported "
                "for data existence checks."
            )

        # If folks use the default existence checks that klio does, we
        # shouldn't get here. But we could if the user implements their
        # own and misconfigures their data outputs.
        if len(self._klio.config.job_config.data.outputs) == 0:
            # raise a runtime error so it actually crashes klio/beam rather than
            # just continue processing elements
            raise RuntimeError(
                "Output data existence checks require output data to be "
                "configured in `klio-job.yaml::job_config.data.outputs`."
            )
        return self._klio.config.job_config.data.outputs[0]


class _KlioGcsDataExistsMixin(object):
    """Mixin for GCS-specific data existence check logic.

    Must be used with _KlioBaseDataExistenceCheck and either
    _KlioInputDataMixin or _KlioOutputDataMixin
    """

    def setup(self):
        self.client = gcsio.GcsIO()

    def exists(self, path):
        return self.client.exists(path)


class _KlioGcsCheckExistsBase(
    _KlioGcsDataExistsMixin, _KlioBaseDataExistenceCheck
):
    """Must be used with either _KlioInputDataMixin or _KlioOutputDataMixin"""

    def process(self, kmsg):
        item = kmsg.data.element
        item_path = self._get_absolute_path(item)
        item_exists = self.exists(item_path)

        state = DataExistState.FOUND
        if not item_exists:
            state = DataExistState.NOT_FOUND

        self._klio.logger.info(
            "%s %s at %s"
            % (
                self.DIRECTION_PFX.value.title(),
                DataExistState.to_str(state),
                item_path,
            )
        )

        # double tag for easier user interface, i.e. pcoll.found vs pcoll.true
        yield pvalue.TaggedOutput(state.value, kmsg.SerializeToString())

# Copyright 2019-2020 Spotify AB
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
import importlib
import json
import logging


import attr

logger = logging.getLogger("klio")


class KlioIODirection(enum.Enum):
    INPUT = 1
    OUTPUT = 2


class KlioIOType(enum.Enum):
    EVENT = 1
    DATA = 2


class KlioJobMode(enum.Enum):
    STREAMING = 1
    BATCH = 2


def supports(*type_directions):
    """Decorator for subclasses of KlioIOConfig to indicate what IO types and
    directions the Config supports.  In many cases the same config object can
    be used for configuring either input or output, and likewise event I/O and
    data I/O.  Adding respective `KlioIOType` and `KlioIODirection` values via
    this decorator will inform Klio about which class to use when parsing
    config.

    """

    def wrapper(cls):
        io_types = [t for t in type_directions if isinstance(t, KlioIOType)]
        io_directions = [
            t for t in type_directions if isinstance(t, KlioIODirection)
        ]
        # IMPORTANT - new lists must be created here, otherwise all subclasses
        # end up sharing the same lists
        cls.SUPPORTED_TYPES = [t for t in cls.SUPPORTED_TYPES]
        cls.SUPPORTED_DIRECTIONS = [d for d in cls.SUPPORTED_DIRECTIONS]
        for t in io_types:
            if t not in cls.SUPPORTED_TYPES:
                cls.SUPPORTED_TYPES.append(t)
        for d in io_directions:
            if d not in cls.SUPPORTED_DIRECTIONS:
                cls.SUPPORTED_DIRECTIONS.append(d)
        return cls

    return wrapper


@attr.attrs
class KlioIOConfig(object):
    io_type = attr.attrib(type=KlioIOType)
    io_direction = attr.attrib(type=KlioIODirection)

    # these must be filled in by subclasses so Klio knows what supports what
    SUPPORTED_TYPES = []
    SUPPORTED_DIRECTIONS = []
    # NOTICE! any new `attr.attrib`s added to this class should be added in
    # `ATTRIBS_TO_SKIP` below in order to be filtered out when calling
    # `as_dict`, leaving only `attr.attrib`s related to the specific
    # config instance
    ATTRIBS_TO_SKIP = ["io_type", "io_direction"]

    @classmethod
    def supports_type(cls, io_type):
        return io_type in cls.SUPPORTED_TYPES

    @classmethod
    def supports_direction(cls, io_direction):
        return io_direction in cls.SUPPORTED_DIRECTIONS

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        copy = config_dict.copy()
        if "type" in copy:
            del copy["type"]
        return cls(*args, **copy, **kwargs)

    def _as_dict(self):
        """Return a dictionary-representation of the config object.

        Useful for instantiating objects where the config keys map 1:1
        to the object init arguments/keyword arguments, i.e.
        KlioReadFromBigQuery(**config.job_config.events.inputs[0].as_dict())
        """
        # filter parameter is used as "to include"/"filter-in",
        # not "to exclude"/"filter-out"
        return attr.asdict(
            self, filter=lambda x, _: x.name not in self.ATTRIBS_TO_SKIP
        )

    def as_dict(self):
        config_dict = self._as_dict()
        # since dicts preserve order by default in py3, let's force
        # type to be first - particularly helpful/useful for dumping
        # config via `klio job config show`
        copy = {"type": self.name}
        copy.update(config_dict)
        return copy

    def to_io_kwargs(self):
        return self._as_dict()


@attr.attrs(frozen=True)
class IOFlags(object):
    io_direction = attr.attrib(type=KlioIODirection)
    io_type = attr.attrib(type=KlioIOType)
    job_mode = attr.attrib(type=KlioJobMode)


@attr.attrs(frozen=True)
class KlioEventInput(KlioIOConfig):
    skip_klio_read = attr.attrib(type=bool)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        # work-around with attrs - since attrs will not allow attributes
        # to be defined without a default after those that have defaults,
        # we're inserting the default value here if the user doesn't have
        # it already in their config
        if "skip_klio_read" not in config_dict:
            copy = config_dict.copy()
            copy["skip_klio_read"] = False
            return super().from_dict(copy, *args, **kwargs)
        return super().from_dict(config_dict, *args, **kwargs)

    def to_io_kwargs(self):
        kwargs = super().to_io_kwargs()
        kwargs.pop("skip_klio_read", None)
        return kwargs


@attr.attrs(frozen=True)
class KlioEventOutput(KlioIOConfig):
    skip_klio_write = attr.attrib(type=bool)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        # work-around with attrs - since attrs will not allow attributes
        # to be defined without a default after those that have defaults,
        # we're inserting the default value here if the user doesn't have
        # it already in their config
        if "skip_klio_write" not in config_dict:
            copy = config_dict.copy()
            copy["skip_klio_write"] = False
            return super().from_dict(copy, *args, **kwargs)
        return super().from_dict(config_dict, *args, **kwargs)

    def to_io_kwargs(self):
        kwargs = super().to_io_kwargs()
        kwargs.pop("skip_klio_write", None)
        return kwargs


@attr.attrs(frozen=True)
class KlioDataIOConfig(KlioIOConfig):
    skip_klio_existence_check = attr.attrib(type=bool)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        # work-around with attrs - since attrs will not allow attributes
        # to be defined without a default after those that have defaults,
        # we're inserting the default value here if the user doesn't have
        # it already in their config
        if "skip_klio_existence_check" not in config_dict:
            copy = config_dict.copy()
            copy["skip_klio_existence_check"] = False
            return super().from_dict(copy, *args, **kwargs)
        return super().from_dict(config_dict, *args, **kwargs)

    def to_io_kwargs(self):
        kwargs = super().to_io_kwargs()
        kwargs.pop("skip_klio_existence_check", None)
        return kwargs


@attr.attrs(frozen=True)
class KlioPubSubConfig(object):
    name = "pubsub"
    topic = attr.attrib(type=str)

    @staticmethod
    def _from_dict(config_dict):
        copy = config_dict.copy()
        copy.pop("data_location", None)
        return copy


@supports(KlioIODirection.INPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioPubSubEventInput(KlioEventInput, KlioPubSubConfig):
    subscription = attr.attrib(type=str)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        config_dict = super()._from_dict(config_dict)
        # work-around with attrs - since attrs will not allow attributes
        # to be defined without a default after those that have defaults,
        # we're inserting the default value here if the user doesn't have
        # it already in their config
        if "topic" not in config_dict:
            config_dict["topic"] = None
        if "subscription" not in config_dict:
            config_dict["subscription"] = None
        return super().from_dict(config_dict, *args, **kwargs)

    @subscription.validator
    def __assert_topic_subscription(self, attribute, value):
        # either topic or subscription is required
        if not self.topic and not self.subscription:
            raise ValueError("One of 'topic', 'subscription' required")

    def to_io_kwargs(self):
        kwargs = super().to_io_kwargs()
        # pubsub only allows either topic or subscription, not both.  We'll
        # prioritize subscription
        if kwargs["topic"] is not None and kwargs["subscription"] is None:
            kwargs.pop("subscription", None)
        else:
            kwargs.pop("topic", None)
        return kwargs


@supports(KlioIODirection.OUTPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioPubSubEventOutput(KlioEventOutput, KlioPubSubConfig):
    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        config_dict = super()._from_dict(config_dict)
        return super().from_dict(config_dict, *args, **kwargs)


class KlioFileConfig(object):
    name = "file"


@attr.attrs(frozen=True)
class KlioReadFileConfig(KlioEventInput, KlioFileConfig):
    file_pattern = attr.attrib(type=str)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        if "location" in config_dict:
            copy = config_dict.copy()
            copy["file_pattern"] = copy.pop("location", None)
            return super().from_dict(copy, *args, **kwargs)
        try:
            return super().from_dict(config_dict, *args, **kwargs)
        except TypeError as e:
            if "file_pattern" in str(e):
                raise KeyError(
                    "I/O configuration for `type: file` is missing 'location' "
                    "key."
                )

    def as_dict(self):
        config_dict = super().as_dict()
        copy = config_dict.copy()
        copy["location"] = copy.pop("file_pattern", None)
        return copy


@attr.attrs(frozen=True)
class KlioWriteFileConfig(KlioEventOutput, KlioFileConfig):
    file_path_prefix = attr.attrib(type=str)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        if "location" in config_dict:
            copy = config_dict.copy()
            copy["file_path_prefix"] = copy.pop("location", None)
            return super().from_dict(copy, *args, **kwargs)
        return super().from_dict(config_dict, *args, **kwargs)

    def as_dict(self):
        config_dict = super().as_dict()
        copy = config_dict.copy()
        copy["location"] = copy.pop("file_path_prefix", None)
        return copy


@supports(KlioIODirection.INPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioReadFileEventConfig(KlioReadFileConfig):
    pass


@supports(KlioIODirection.OUTPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioWriteFileEventConfig(KlioWriteFileConfig):
    pass


@attr.attrs(frozen=True)
@supports(KlioIODirection.INPUT, KlioIOType.DATA)
class KlioFileInputDataConfig(KlioDataIOConfig, KlioFileConfig):
    location = attr.attrib(type=str)
    ping = attr.attrib(type=bool, default=False)
    file_suffix = attr.attrib(type=str, default="")


@attr.attrs(frozen=True)
@supports(KlioIODirection.OUTPUT, KlioIOType.DATA)
class KlioFileOutputDataConfig(KlioDataIOConfig, KlioFileConfig):
    location = attr.attrib(type=str)
    file_suffix = attr.attrib(type=str, default="")
    force = attr.attrib(type=bool, default=False)


class KlioAvroConfig(object):
    name = "avro"


@attr.attrs(frozen=True)
class KlioReadAvroConfig(KlioAvroConfig):
    # TODO: Add validation
    # Potentially could set a general file pattern default, i.e. *.avro, too
    # but either file_pattern or location must be set
    file_pattern = attr.attrib(type=str, default=None)
    location = attr.attrib(type=str, default=None)
    min_bundle_size = attr.attrib(type=int, default=0)
    validate = attr.attrib(type=bool, default=True)


@supports(KlioIODirection.INPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioReadAvroEventConfig(KlioEventInput, KlioReadAvroConfig):
    pass


@attr.attrs(frozen=True)
class KlioWriteAvroConfig(KlioAvroConfig):
    # Either file_path_prefix or location must be set
    # Right now `schema` is also allowed to pass inbut anything other than
    # the default schema cannot make proper use of `write_record`
    file_path_prefix = attr.attrib(type=str, default=None)
    location = attr.attrib(type=str, default=None)
    codec = attr.attrib(type=str, default="deflate")
    file_name_suffix = attr.attrib(type=str, default="")
    num_shards = attr.attrib(type=int, default=0)
    shard_name_template = attr.attrib(type=str, default=None)
    mime_type = attr.attrib(type=str, default="application/x-avro")


@supports(KlioIODirection.OUTPUT, KlioIOType.EVENT)
@attr.attrs(frozen=True)
class KlioWriteAvroEventConfig(KlioEventOutput, KlioWriteAvroConfig):
    pass


# TODO: integrate into @dsimon's converter logic once his PR#154 is merged
def _convert_bigquery_input_coder(coder_str):
    # direct runner seems to call this multiple times, prob with pickling;
    # subsequent times seem to call with a None value (not sure why...)
    if coder_str is None:
        return

    coder_path_stems = coder_str.split(".")
    coder_kls_str = coder_path_stems.pop()
    module_str = ".".join(coder_path_stems)
    module = importlib.import_module(module_str)  # should raise if not avail
    return getattr(module, coder_kls_str)  # should raise if not avail


@attr.attrs(frozen=True)
class KlioBigQueryConfig(object):
    name = "bq"
    project = attr.attrib(type=str, default=None)
    dataset = attr.attrib(type=str, default=None)
    table = attr.attrib(type=str, default=None)


@attr.attrs(frozen=True)
@supports(KlioIODirection.INPUT, KlioIOType.EVENT)
class KlioBigQueryEventInput(KlioEventInput, KlioBigQueryConfig):
    # Set to true by default to fail early (check table existence before
    # starting pipeline) (Beam sets it to false by default but suggests it
    # to be set to true in most scenarios)
    validate = attr.attrib(type=bool, default=True)
    # TODO: use @dsimon's converter logic once his PR#154 is merged in
    coder = attr.attrib(
        type=str, default=None, converter=_convert_bigquery_input_coder
    )
    kms_key = attr.attrib(type=str, default=None)
    # Optional, but only applies to project+dataset+table;
    # Klio handles filtering data after a `SELECT *` query
    klio_message_columns = attr.attrib(type=list, default=None)
    # Mutually exclusive with project+dataset+table & klio_message_columns
    query = attr.attrib(type=str, default=None)
    use_standard_sql = attr.attrib(type=bool, default=False)
    flatten_results = attr.attrib(type=bool, default=True)

    @klio_message_columns.validator
    def __assert_project_dataset_table(self, attribute, value):
        has_project_dataset_table = all(
            [self.project, self.dataset, self.table]
        )
        if value is not None:
            if not has_project_dataset_table:
                raise ValueError(
                    "Must include `project`, `dataset` and `table` if "
                    "selecting `columns` for BigQuery."
                )

    @query.validator
    def __assert_query_or_project_dataset_table(self, attribute, value):
        has_project_dataset_table = any(
            [self.project, self.dataset, self.table]
        )
        if value is not None and has_project_dataset_table:
            raise ValueError(
                "`query` is mutually exclusive with `project`, `dataset` "
                "and `table`."
            )

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        if "columns" in config_dict:
            copy = config_dict.copy()
            copy["klio_message_columns"] = copy.pop("columns", None)
            return super().from_dict(copy, *args, **kwargs)
        return super().from_dict(config_dict, *args, **kwargs)

    def as_dict(self):
        config_dict = super().as_dict()
        copy = config_dict.copy()
        copy["columns"] = copy.pop("klio_message_columns", None)
        return copy


def _convert_bigquery_output_schema(schema):
    if isinstance(schema, dict):
        return schema
    return json.loads(schema)


@attr.attrs(frozen=True)
@supports(KlioIODirection.OUTPUT, KlioIOType.EVENT)
class KlioBigQueryEventOutput(KlioEventOutput, KlioBigQueryConfig):
    # schema field is optional; assumes form of
    # {"fields": [{"name": ...,"type": ..., "mode": ...}, ... ]
    schema = attr.attrib(
        type=dict, converter=_convert_bigquery_output_schema, default=None
    )
    create_disposition = attr.attrib(type=str, default="CREATE_IF_NEEDED")
    write_disposition = attr.attrib(type=str, default="WRITE_EMPTY")

    @schema.validator
    def check(self, attribute, value):
        def valid_field(field_dict):
            is_dict = isinstance(field_dict, dict)
            required_keys = [
                "name",
                "type",
                "mode",
            ]  # are all of these required?
            return is_dict and all(k in field_dict for k in required_keys)

        def contains_invalid_fields(field_list):
            return any(valid_field(f) is False for f in field_list)

        has_fields = value.get("fields")
        if not has_fields or contains_invalid_fields(value.get("fields", [])):
            raise ValueError(
                "Must be a dict with the key `fields` set to a list of"
                "dict, each of which have the keys "
                "`name`, `type`, and `mode`"
            )


@attr.attrs(frozen=True)
class KlioGCSConfig(KlioIOConfig):
    name = "gcs"
    location = attr.attrib(type=str)

    @staticmethod
    def _from_dict(config_dict):
        copy = config_dict.copy()
        copy.pop("topic", None)
        copy.pop("subscription", None)
        data_location = copy.pop("data_location", None)
        copy["location"] = copy.get("location", data_location)
        return copy


@attr.attrs(frozen=True)
@supports(KlioIODirection.INPUT, KlioIOType.DATA)
class KlioGCSInputDataConfig(KlioDataIOConfig, KlioGCSConfig):
    file_suffix = attr.attrib(type=str, default="")
    ping = attr.attrib(type=bool, default=False)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        config_dict = super()._from_dict(config_dict)
        return super().from_dict(config_dict, *args, **kwargs)


@attr.attrs(frozen=True)
@supports(KlioIODirection.OUTPUT, KlioIOType.DATA)
class KlioGCSOutputDataConfig(KlioDataIOConfig, KlioGCSConfig):
    file_suffix = attr.attrib(type=str, default="")
    force = attr.attrib(type=bool, default=False)

    @classmethod
    def from_dict(cls, config_dict, *args, **kwargs):
        config_dict = super()._from_dict(config_dict)
        return super().from_dict(config_dict, *args, **kwargs)

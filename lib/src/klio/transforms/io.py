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

import json
import os

import apache_beam as beam
from apache_beam.io import avroio as beam_avroio
from apache_beam.io.gcp import bigquery as beam_bq
from fastavro import parse_schema

from klio_core.proto import klio_pb2

from klio.transforms import core


class BaseKlioIOException(Exception):
    """Base IO exception."""


class KlioMissingConfiguration(BaseKlioIOException):
    """Required configuration is missing."""


class _KlioReadWrapper(beam.io.Read):
    """Klio-ified `beam.io.Read` class.

    Handle the case if a user invokes an already-wrapped Klio IO
    transform (via `_KlioTransformMixin`) with `beam.io.Read`, as this
    is not possible to double-wrap.
    """

    def __init__(self, *args, **kwargs):
        if not len(args):
            # I would think we'd never get here, but this is just in case
            # something real funky happens, like this semi-private class
            # is not instantiated with an IO transform
            super(_KlioReadWrapper, self).__init__(*args, **kwargs)

        self.__wrapped_transform = args[0].__class__.__name__
        super(_KlioReadWrapper, self).__init__(*args, **kwargs)

    def is_bounded(self, *args, **kwargs):
        # beam.io.Read invokes beam.io.Read.source.is_bounded
        # (beam.io.Read.source == self/this instance) and throw an
        # AttributeError if is_bounded not defined. Here we define
        # the method but raise our own expection telling the user not to
        # invoke an alread-wrapped Klio IO Transform with `beam.io.Read()`
        raise TypeError(
            "Error reading from `{}`. If the transform was wrapped with "
            "`apache_beam.io.Read`, remove that wrapper and try again.".format(
                self.__wrapped_transform
            )
        )


class _KlioWrapIOMetaclass(type):
    def __call__(self, *args, **kwargs):
        # Some IO transforms require invocation to be wrapped with
        # `beam.io.Read()`, and others do not. This allows for the API
        # to be the same no matter what by wrapping `beam.io.Read` if
        # the transform requires it. If a user wraps a Klio IO transform
        # again (i.e. `beam.io.Read(_AlreadyWrappedKlioTransform))`), a
        # human-friendly `TypeError` will raise (see
        # `_KlioReadWrapper.is_bounded`)
        if self._REQUIRES_IO_READ_WRAP:
            return _KlioReadWrapper(
                super(_KlioWrapIOMetaclass, self).__call__(*args, **kwargs)
            )
        return super(_KlioWrapIOMetaclass, self).__call__(*args, **kwargs)


class _KlioTransformMixin(metaclass=_KlioWrapIOMetaclass):
    """Common properties for klio v2 IO transforms."""

    # whether or not the transform needs to be invoked by beam.io.Read()
    _REQUIRES_IO_READ_WRAP = False


# TODO: merge with helpers.KlioMessageCounter since it's the same logic
class _KlioIOCounter(beam.DoFn):
    """Internal helper transform to count elements to/from I/O transforms.

    This should be used right after a read transform, or right before a
    write transform. Since it is a DoFn transform, it needs to be invoked
    via ``beam.ParDo`` when used. Examples:

    .. code-block:: python

        class MyReadCompositeTransform(beam.PTransform):
            def __init__(self, *args, **kwargs):
                self.reader = MyReadTransform(*args, **kwargs)
                self.counter = _KlioIOCounter("read", "MyReadTransform")

            def expand(self, pbegin):
                return (
                    pbegin
                    | "Read Input" >> self.reader
                    | "Read Counter" >> beam.ParDo(self.counter)
                )

        class MyWriteCompositeTransform(beam.PTransform):
            def __init__(self, *args, **kwargs):
                self.writer = MyWriteTransform(*args, **kwargs)
                self.counter = _KlioIOCounter("write", "MyWriteTransform")

            def expand(self, pbegin):
                return (
                    pbegin
                    | "Write Counter" >> beam.ParDo(self.counter)
                    | "Write Output" >> self.writer
                )

    Args:
        direction (str): direction of the counter. Choices: ``read``,
            ``write``.
        bind_transform (str): Name of transform to bind the counter to.
    """

    def __init__(self, direction, bind_transform):
        assert direction in ("read", "write")
        self.direction = direction
        self.bind_transform = bind_transform

    def setup(self):
        ctx = core.KlioContext()
        self.io_counter = ctx.metrics.counter(
            f"kmsg-{self.direction}", transform=self.bind_transform
        )

    def process(self, item):
        self.io_counter.inc()
        yield item


class _KlioReadFromTextSource(beam.io.textio._TextSource):
    """Parses a text file as newline-delimited elements.
       Supports newline delimiters '\n' and '\r\n

    Returns:
        (str) KlioMessage serialized as a string
    """

    def read_records(self, file_name, range_tracker):
        records = super(_KlioReadFromTextSource, self).read_records(
            file_name, range_tracker
        )

        for record in records:
            record_as_bytes = record.encode("utf-8")
            message = klio_pb2.KlioMessage()
            message.version = klio_pb2.Version.V2
            message.metadata.intended_recipients.anyone.SetInParent()
            message.data.element = record_as_bytes
            yield message.SerializeToString()


class _KlioReadFromText(beam.io.ReadFromText, _KlioTransformMixin):

    _source_class = _KlioReadFromTextSource


class KlioReadFromText(beam.PTransform):
    """Read from a local or GCS file with each new line as a
    ``KlioMessage.data.element``.
    """

    def __init__(self, *args, **kwargs):
        self._reader = _KlioReadFromText(*args, **kwargs)
        self.__counter = _KlioIOCounter("read", "KlioReadFromText")

    def expand(self, pbegin):
        return (
            pbegin
            | "KlioReadFromText" >> self._reader
            | "Read Counter" >> beam.ParDo(self.__counter)
        )


class _KlioReadFromBigQueryMapper(object):
    """Wrapper class to provide a ``beam.Map`` object that converts a row of a
    ``ReadFromBigQuery`` to a properly formatted ``KlioMessage``.
    """

    def __init__(self, klio_message_columns=None):
        self.__klio_message_columns = klio_message_columns

    def _generate_klio_message(self):
        message = klio_pb2.KlioMessage()
        message.version = klio_pb2.Version.V2
        message.metadata.intended_recipients.anyone.SetInParent()

        # TODO: this is where we should add (relevant) KlioMessage.metadata;
        # (1) One thing to figure out is the klio_pb2.KlioJob definition,
        # particularly the JobInput definition, in light of KlioConfig v2.
        # Once that's figured out, we should at least populate the
        # job audit log.
        # (2) Another thing to figure out is force/ping. In streaming, messages
        # are individually marked as force or ping when needed. However,
        # users aren't able to tag individual messages generated from a row
        # of BQ data as force/ping, and it's probably very difficult for us
        # to provide a way to do that. So, should we allow users to at least
        # globally set force/ping on their event input config in klio-job.yaml?
        # Potentially.
        return message

    def _map_row_element(self, row):
        # NOTE: this assumes that the coder being used (default is
        # beam.io.gcp.bigquery_tools.RowAsDictJsonCoder, otherwise set in
        # klio-job.yaml) is JSON serializable (since the default is just
        # a plain dictionary). This assumption might break if someone
        # provides a different coder.
        # NOTE: We need to have the row elements be bytes, so if it is
        # a dictionary, we json.dumps into a str to convert to bytes,
        # but that may need to change if we want to support other coders
        data = {}
        if self.__klio_message_columns:
            if len(self.__klio_message_columns) == 1:
                data = row[self.__klio_message_columns[0]]

            else:
                for key, value in row.items():
                    if key in self.__klio_message_columns:
                        data[key] = value
                data = json.dumps(data)

        else:
            data = json.dumps(row)
        return data

    def _map_row(self, row):
        message = self._generate_klio_message()
        message.data.element = bytes(self._map_row_element(row), "utf-8")
        return message.SerializeToString()

    def as_beam_map(self):
        return "Convert to KlioMessage" >> beam.Map(self._map_row)


# Note: copy-pasting the docstrings of `ReadFromBigQuery` so that we can
# include our added parameter (`klio_message_columns`) in the API
# documentation (via autodoc). If we don't do this, then just the parent
# documentation will be shown, excluding our new parameter.
class KlioReadFromBigQuery(beam.PTransform, _KlioTransformMixin):
    """Read data from BigQuery.

    This PTransform uses a BigQuery export job to take a snapshot of the table
    on GCS, and then reads from each produced file. File format is Avro by
    default.

    Args:
        table (str, callable, ValueProvider): The ID of the table, or a callable
            that returns it. The ID must contain only letters ``a-z``, ``A-Z``,
            numbers ``0-9``, or underscores ``_``. If dataset argument is
            :data:`None` then the table argument must contain the entire table
            reference specified as: ``'DATASET.TABLE'``
            or ``'PROJECT:DATASET.TABLE'``.
            If it's a callable, it must receive one argument representing an
            element to be written to BigQuery, and return
            a TableReference, or a string table name as specified above.
        dataset (str): The ID of the dataset containing this table or
            :data:`None` if the table reference is specified entirely by
            the table argument.
        project (str): The ID of the project containing this table.
        klio_message_columns (list): A list of fields (``str``) that should
            be assigned to ``KlioMessage.data.element``.

            .. note::

                If more than one field is provided, the results including the
                column names will be serialized to JSON before assigning to
                ``KlioMessage.data.element``. (e.g. ``'{"field1": "foo",
                "field2": bar"}'``). If only one field is provided, just the
                value will be assigned to ``KlioMessage.data.element``.

        query (str, ValueProvider): A query to be used instead of arguments
            table, dataset, and project.
        validate (bool): If :data:`True`, various checks will be done when
            source gets initialized (e.g., is table present?).
            This should be :data:`True` for most scenarios
            in order to catch errors as early as possible
            (pipeline construction instead of pipeline execution).
            It should be :data:`False` if the table is created during pipeline
            execution by a previous step.
        coder (~apache_beam.coders.coders.Coder): The coder for the table
            rows. If :data:`None`, then the default coder is
            _JsonToDictCoder, which will interpret every row as a JSON
            serialized dictionary.
        use_standard_sql (bool): Specifies whether to use BigQuery's standard
            SQL dialect for this query. The default value is :data:`False`.
            If set to :data:`True`, the query will use BigQuery's updated SQL
            dialect with improved standards compliance.
            This parameter is ignored for table inputs.
        flatten_results (bool): Flattens all nested and repeated fields in the
            query results. The default value is :data:`True`.
        kms_key (str): Optional Cloud KMS key name for use when creating new
            temporary tables.
        gcs_location (str, ValueProvider): The name of the Google Cloud Storage
            bucket where the extracted table should be written as a string or
            a :class:`~apache_beam.options.value_provider.ValueProvider`. If
            :data:`None`, then the temp_location parameter is used.
        bigquery_job_labels (dict): A dictionary with string labels to be passed
            to BigQuery export and query jobs created by this transform. See:
            https://cloud.google.com/bigquery/docs/reference/rest/v2/\
Job#JobConfiguration
        use_json_exports (bool): By default, this transform works by exporting
            BigQuery data into Avro files, and reading those files. With this
            parameter, the transform will instead export to JSON files.
            JSON files are slower to read due to their larger size.
            When using JSON exports,
            the BigQuery types for DATE, DATETIME, TIME, and TIMESTAMP will be
            exported as strings.

            This behavior is consistent with BigQuerySource.
            When using Avro exports,
            these fields will be exported as native Python
            types (datetime.date, datetime.datetime, datetime.datetime,
            and datetime.datetime respectively). Avro exports are recommended.
            To learn more about BigQuery types, and Time-related type
            representations,
            see:
            https://cloud.google.com/bigquery/docs/reference/standard-sql/\
data-types
            To learn more about type conversions between BigQuery and Avro, see:
            https://cloud.google.com/bigquery/docs/loading-data-cloud-\
storage-avro#avro_conversions
     """

    def __init__(self, *args, klio_message_columns=None, **kwargs):
        self._reader = beam_bq.ReadFromBigQuery(*args, **kwargs)
        self.__mapper = _KlioReadFromBigQueryMapper(klio_message_columns)
        self.__counter = _KlioIOCounter("read", "KlioReadFromBigQuery")

    def expand(self, pcoll):
        return (
            pcoll
            | "ReadFromBigQuery" >> self._reader
            | "Create KlioMessage" >> self.__mapper.as_beam_map()
            | "Read Counter" >> beam.ParDo(self.__counter)
        )


class KlioWriteToBigQuery(beam.PTransform, _KlioTransformMixin):
    """Writes to BigQuery table with each row as ``KlioMessage.data.element``.
    """

    # Note: Not using BigQuerySink due to it only being available for
    # batch. See https://beam.apache.org/releases/pydoc/2.22.0/
    # apache_beam.io.gcp.bigquery.html?highlight=bigquerysink
    # #apache_beam.io.gcp.bigquery.BigQuerySink

    _REQUIRES_IO_READ_WRAP = False

    def __init__(self, *args, **kwargs):
        self._writer = beam.io.WriteToBigQuery(*args, **kwargs)
        self.__counter = _KlioIOCounter("write", "KlioWriteToBigQuery")

    def __unwrap(self, encoded_element):
        message = klio_pb2.KlioMessage()
        message.ParseFromString(encoded_element)
        data = json.loads(message.data.payload)

        return data

    def expand(self, pcoll):
        return (
            pcoll
            | "Write Counter" >> beam.ParDo(self.__counter)
            | "Serialize Klio Message" >> beam.Map(self.__unwrap)
            | "WriteToBigQuery" >> self._writer
        )


class _KlioTextSink(beam.io.textio._TextSink):
    """A :class:`~apache_beam.transforms.ptransform.PTransform`
       for writing to text files. Takes a PCollection of KlioMessages
       and writes the elements to a textfile
    """

    def write_record(self, file_handle, encoded_element):
        """Writes a single encoded record.
        Args:
            file_handle (str): a referential identifier that points to an
                audio file found in the configured output data location.
            encoded_element (KlioMessage): KlioMessage
        """
        message = klio_pb2.KlioMessage()
        message.ParseFromString(encoded_element)
        record = message.data.element
        super(_KlioTextSink, self).write_encoded_record(file_handle, record)


class _KlioWriteToText(beam.io.textio.WriteToText):
    def __init__(self, *args, **kwargs):
        self._sink = _KlioTextSink(*args, **kwargs)


class KlioWriteToText(beam.PTransform):
    """Write to a local or GCS file with each new line as
    ``KlioMessage.data.element``.
    """

    def __init__(self, *args, **kwargs):
        self.__writer = _KlioWriteToText(*args, **kwargs)
        self.__counter = _KlioIOCounter("write", "KlioWriteToText")

    def expand(self, pcoll):
        return (
            pcoll
            | "Write Counter" >> beam.ParDo(self.__counter)
            | "KlioWriteToText" >> self.__writer
        )


# note: fast avro is default for py3 on beam
class _KlioFastAvroSource(beam_avroio._FastAvroSource):
    def read_records(self, file_name, range_tracker):
        records = super(_KlioFastAvroSource, self).read_records(
            file_name=file_name, range_tracker=range_tracker
        )
        for record in records:
            message = klio_pb2.KlioMessage()
            message.version = klio_pb2.Version.V2
            message.metadata.intended_recipients.anyone.SetInParent()
            # If an element is sent then we set the element
            # to handle event reading
            # If "element" is not present then we stuff the record
            # into the message element
            message.data.element = (
                record["element"]
                if "element" in record
                else bytes(json.dumps(record).encode("utf-8"))
            )
            yield message.SerializeToString()


# define an I/O transform using the klio-specific avro source
# note: fast avro is default for py3 on beam
# Note: copy-pasting the docstrings of `ReadFromAvro` so that we can
# include our added parameter (`location`) in the API
# documentation (via autodoc) and drop `use_fastavro` since we default to
# True. If we don't do this, then just the parent documentation will be shown,
# excluding our new parameter and including an unavailable parameter
# (`location` and `use_fastavro` respectively)
class _KlioReadFromAvro(beam.io.ReadFromAvro):

    _REQUIRES_IO_READ_WRAP = True

    def __init__(
        self,
        file_pattern=None,
        location=None,
        min_bundle_size=0,
        validate=True,
    ):
        file_pattern = self._get_file_pattern(file_pattern, location)

        super(_KlioReadFromAvro, self).__init__(
            file_pattern=file_pattern,
            min_bundle_size=min_bundle_size,
            validate=validate,
            use_fastavro=True,
        )

        self._source = _KlioFastAvroSource(
            file_pattern, min_bundle_size, validate=validate
        )

    def _get_file_pattern(self, file_pattern, location):
        # TODO: this should be a validator in klio_core.config
        if not any([file_pattern, location]):
            raise KlioMissingConfiguration(
                "Must configure at least one of the following keys when "
                "reading from avro: `file_pattern`, `location`."
            )

        if all([file_pattern, location]):
            file_pattern = os.path.join(location, file_pattern)

        elif file_pattern is None:
            file_pattern = location

        return file_pattern


class KlioReadFromAvro(beam.PTransform):
    """Read avro from a local directory or GCS bucket.

    Data from avro is dumped into JSON and assigned to ``KlioMessage.data.
    element``.

    ``KlioReadFromAvro`` is the default read for event input config type avro.
    However, ``KlioReadFromAvro`` can also be called explicity in a pipeline.

    Example pipeline reading in elements from an avro file:

    .. code-block:: python

        def run(pipeline, config):
            initial_data_path = os.path.join(DIRECTORY_TO_AVRO, "twitter.avro")
            pipeline | io_transforms.KlioReadFromAvro(
                    file_pattern=initial_data_path
            ) | beam.ParDo(transforms.HelloKlio())

    Args:
      file_pattern (str): the file glob to read.
      location (str): local or GCS path of file(s) to read.
      min_bundle_size (int): the minimum size in bytes, to be considered when
        splitting the input into bundles.
      validate (bool): flag to verify that the files exist during the pipeline
        creation time.
    """

    def __init__(self, *args, **kwargs):
        self._reader = _KlioReadFromAvro(*args, **kwargs)
        self.__counter = _KlioIOCounter("read", "KlioReadFromAvro")

    def expand(self, pbegin):
        return (
            pbegin
            | "KlioReadFromAvro" >> self._reader
            | "Read Counter" >> beam.ParDo(self.__counter)
        )


# note: fast avro is default for py3 on beam
class _KlioFastAvroSink(beam_avroio._FastAvroSink):
    def write_record(self, writer, encoded_element):
        message = klio_pb2.KlioMessage()
        message.ParseFromString(encoded_element)
        record = {"element": message.data.element}
        super(_KlioFastAvroSink, self).write_record(
            writer=writer, value=record
        )


# Note of caution: In the past problems have arisen due to
# changes to internal beam classes.
# If this occurs, consider writing this as a custom PTransform
# that bundles a ``beam.Map`` with the standard ``WriteToAvro``
# Refer to ``KlioReadFromBigQuery`` as an example.
class _KlioWriteToAvro(beam.io.WriteToAvro):
    KLIO_SCHEMA_OBJ = {
        "namespace": "klio.avro",
        "type": "record",
        "name": "KlioMessage",
        "fields": [{"name": "element", "type": "bytes"}],
    }

    def __init__(
        self,
        file_path_prefix=None,
        location=None,
        schema=parse_schema(KLIO_SCHEMA_OBJ),
        codec="deflate",
        file_name_suffix="",
        num_shards=0,
        shard_name_template=None,
        mime_type="application/x-avro",
    ):

        file_path = self._get_file_path(file_path_prefix, location)

        super(_KlioWriteToAvro, self).__init__(
            file_path_prefix=file_path,
            schema=schema,
            codec=codec,
            file_name_suffix=file_name_suffix,
            num_shards=num_shards,
            shard_name_template=shard_name_template,
            mime_type=mime_type,
            use_fastavro=True,
        )

        self._sink = _KlioFastAvroSink(
            file_path,
            schema,
            codec,
            file_name_suffix,
            num_shards,
            shard_name_template,
            mime_type,
        )

    def _get_file_path(self, file_path_prefix, location):
        # TODO: this should be a validator in klio_core.config
        if not any([file_path_prefix, location]):
            raise KlioMissingConfiguration(
                "Must configure at least one of the following keys when "
                "writing to avro: `file_path_prefix`, `location`."
            )

        if all([file_path_prefix, location]):
            file_path_prefix = os.path.join(location, file_path_prefix)

        elif file_path_prefix is None:
            file_path_prefix = location

        return file_path_prefix


class KlioWriteToAvro(beam.PTransform):
    """Write avro to a local directory or GCS bucket.

    ``KlioMessage.data.element`` data is parsed out
    and dumped into avro format.

    ``KlioWriteToAvro`` is the default write for event output config type avro.
    However, ``KlioWriteToAvro`` can also be called explicity in a pipeline.

    Example pipeline for writing elements to an avro file:

    .. code-block:: python

        def run(input_pcol, config):
            output_gcs_location = "gs://test-gcs-location"
            return (
                input_pcol
                | beam.ParDo(HelloKlio())
                | transforms.io.KlioWriteToAvro(location=output_gcs_location)
            )

    Args:
      file_path_prefix (str): The file path to write to
      location (str): local or GCS path to write to
      schema (str): The schema to use, as returned by avro.schema.parse
      codec (str): The codec to use for block-level compression.
            defaults to 'deflate'
      file_name_suffix (str): Suffix for the files written.
      num_shards (int): The number of files (shards) used for output.
      shard_name_template (str): template string for shard number and count
      mime_type (str): The MIME type to use for the produced files.
        Defaults to "application/x-avro"
    """

    def __init__(self, *args, **kwargs):
        self._writer = _KlioWriteToAvro(*args, **kwargs)
        self.__counter = _KlioIOCounter("write", "KlioWriteToAvro")

    def expand(self, pcoll):
        return (
            pcoll
            | "Write Counter" >> beam.ParDo(self.__counter)
            | "KlioWriteToAvro" >> self._writer
        )


class KlioReadFromPubSub(beam.PTransform):
    """Read from a Google Pub/Sub topic or subscription.

    Args:
        topic (str): Cloud Pub/Sub topic in the form
            ``projects/<project>/topics/<topic>``. If provided,
            ``subscription`` must be ``None``.
        subscription (str): Existing Cloud Pub/Sub subscription to use in the
            form ``projects/<project>/subscriptions/<subscription>``. If not
            specified, a temporary subscription will be created from the
            specified topic. If provided, ``topic`` must be ``None``.
        id_label (str): The attribute on incoming Pub/Sub messages to use as a
            unique record identifier. When specified, the value of this
            attribute (which can be any string that uniquely identifies the
            record) will be used for deduplication of messages. If not
            provided, we cannot guarantee that no duplicate data will be
            delivered on the Pub/Sub stream. In this case, deduplication of
            the stream will be strictly best effort.
        with_attributes (bool): With ``True``, output elements will be
            :class:`PubsubMessage <apache_beam.io.gcp.pubsub.PubsubMessage>`
            objects. With ``False``, output elements will be of type ``bytes``
            (message data only). Defaults to ``False``.
        timestamp_attribute (str): Message value to use as element timestamp.
            If ``None``, uses message publishing time as the timestamp.
            Timestamp values should be in one of two formats: (1) A numerical
            value representing the number of milliseconds since the Unix epoch.
            (2) A string in RFC 3339 format, UTC timezone. Example:
            ``2015-10-29T23:41:41.123Z``. The sub-second component of the
            timestamp is optional, and digits beyond the first three (i.e.,
            time units smaller than milliseconds) may be ignored.

    """

    def __init__(self, *args, **kwargs):
        self._reader = beam.io.ReadFromPubSub(*args, **kwargs)
        self.__counter = _KlioIOCounter("read", "KlioReadFromPubSub")

    def expand(self, pbegin):
        return (
            pbegin
            | "Read from PubSub" >> self._reader
            | "Read Counter" >> beam.ParDo(self.__counter)
        )


class KlioWriteToPubSub(beam.PTransform):
    """Write to a Google Pub/Sub topic.

    Args:
        topic (str): Cloud Pub/Sub topic in the form
            ``/topics/<project>/<topic>``.
        with_attributes (bool): With ``True``, input elements will be
            :class:`PubsubMessage <apache_beam.io.gcp.pubsub.PubsubMessage>`
            objects. With ``False``, input elements will be of type ``bytes``
            (message data only). Defaults to ``False``.
        id_label (str): If set, will set an attribute for each Cloud Pub/Sub
            message with the given name and a unique value. This attribute
            can then be used in a ReadFromPubSub PTransform to deduplicate
            messages.
        timestamp_attribute (str): If set, will set an attribute for each
            Cloud Pub/Sub message with the given name and the message's
            publish time as the value.
    """

    def __init__(self, *args, **kwargs):
        self._writer = beam.io.WriteToPubSub(*args, **kwargs)
        self.__counter = _KlioIOCounter("write", "KlioWriteToPubSub")

    def expand(self, pcoll):
        return (
            pcoll
            | "Write Counter" >> beam.ParDo(self.__counter)
            | "Write To PubSub" >> self._writer
        )

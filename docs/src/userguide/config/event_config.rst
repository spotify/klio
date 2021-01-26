.. _event-config:

Event Configuration
===================

Input
-----

.. _event-config-pubsub:

Google Pub/Sub
^^^^^^^^^^^^^^

*Mode: streaming*

Consuming KlioMessages from `Google Pub/Sub`_ is supported for all runners.

Example configuration for `Google Pub/Sub`_:

.. code-block:: yaml

    name: my-cool-job
    pipeline_options:
      streaming: True
    job_config:
        events:
            inputs:
                - type: pubsub
                  topic: my-parent-output-topic
                  subscription: my-input-subscription

.. option:: job_config.events.inputs[].type

    Value: ``pubsub``

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.events.inputs[].topic STR

    The Google Pub/Sub topic to which the job will subscribe when started.

    KlioMessages can be published to this topic via ``klio message publish`` or from a parent
    Klio job.

    While optional, if KlioMessages are published via ``klio message publish``, a topic is
    required. A ``topic`` is also required if no ``subscription`` is specified.

    | **Runner**: Dataflow, Direct
    | *Optional*

    .. warning::

        When only specifying a topic, KlioMessages will be lost between jobs as Beam creates
        a temporary subscription on startup.

    .. note::

        Multiple jobs can subscribe to the same topic.

        If each job has a unique subscription to the topic, they will each receive the same
        ``KlioMessage`` published to that topic. This is useful for multiple dependencies on a
        parent job, or setting up staging/canary environments.

        If each job uses the same subscription to the topic, only one job will process any given
        ``KlioMessage``.

.. option:: job_config.events.inputs[].subscription STR

    The Google Pub/Sub subscription (correlated to the ``topic``, if configured) from which Klio
    will read.

    A ``subscription`` is required if a ``topic`` is not specified.

    | **Runner**: Dataflow, Direct
    | *Optional*

    .. note::

        Multiple jobs can subscribe to the same topic.

        If each job has a unique subscription to the topic, they will each receive the same
        ``KlioMessage`` published to that topic. This is useful for multiple dependencies on a
        parent job, or setting up staging/canary environments.

        If each job uses the same subscription to the topic, only one job will process any given
        ``KlioMessage``.


.. option:: job_config.events.inputs[].skip_klio_read BOOL

    Inherited from :ref:`global event input config <skip-klio-read>`.


.. _event-config-bigquery:

Google BigQuery
^^^^^^^^^^^^^^^

*Mode: Batch*

Consuming KlioMessages from `Google BigQuery`_ is supported for all runners.

Klio allows the user to specify events using either queries or columns from a table. Tables can be specified either though a table reference in the `table` field (e.g. ``'DATASET.TABLE'`` or ``'PROJECT:DATASET.TABLE'``) or by filling in the fields for `project`, `dataset`, and `table`. If the columns representing the event input are not specified through the `columns` field, all columns from the table are returned.  Note that the user cannot specify both a `query` and a `table` (with its associated `project`, `dataset`, and `columns`); they are mutually exclusive.


Example configuration for `Google BigQuery`_, by specifying a table:

.. code-block:: yaml

    name: my-cool-job
    pipeline_options:
        streaming: True
    job_config:
        events:
            inputs:
                - type: bq
                columns:
                - entity_id
                dataset: bigquery_dataset
                project: gcp_project
                table: bigquery_table


Example configuration for `Google BigQuery`_, by specifying a query in the BigQuery dialect:

.. code-block:: yaml

    name: my-cool-job
    pipeline_options:
        streaming: True
    job_config:
    events:
        inputs:
            - type: bq
              query: |
                  SELECT * FROM [gcp_project.bigquery_dataset.bigquery_table]

.. option:: job_config.events.inputs[].type

    Value: ``bq``

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.events.inputs[].columns[]

    Not applicable if ``query`` is specified.

    A list of strings specifying the columns to read events from.
    If only one column is provided, the value will be returned as a bytestring.
    If no columns are specified (meaning that all columns are selected),
    or if more than one column is specified,
    the results including the column names will be serialized to a JSON bytestring,
    e.g. ``'{"field1": "foo", "field2": bar"}'``.


    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].columns[].<column> STR

    A column name in the table or query result used to build the event input from.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].table STR

    Name of the table to use for event input.

    The ID must contain only letters ``a-z``, ``A-Z``, numbers ``0-9``,
    or underscores ``_``.

    If dataset and query arguments are not specified,  then the table argument must
    contain the entire table reference specified as:
    ``'DATASET.TABLE'`` or ``'PROJECT:DATASET.TABLE'``.

    | **Runner**: Dataflow, Direct
    | *Required when using project, dataset, table, and columns to specify event inputs*

.. option:: job_config.events.inputs[].dataset STR

    Name of the event input table's dataset.

    Ignored if the table reference is fully specified by the table argument or
    if a query is specified.

    | **Runner**: Dataflow, Direct
    | *Required when using project, dataset, table, and columns to specify event inputs
       and table reference does not include dataset*

.. option:: job_config.events.inputs[].project STR

    Name of the event input table's project.

    Ignored if the table reference is fully specified by the table argument or
    if a query is specified.

    | **Runner**: Dataflow, Direct
    | *Required when using project, dataset, table, and columns to specify event inputs
       and table reference does not include project*

.. option:: job_config.events.inputs[].query STR

    Query string supplying the columns. Mutually exclusive with specifying the
    ``table`` field.

    | **Runner**: Dataflow, Direct
    | *Required if project, dataset, table and columns were not used to specify event inputs*

.. option:: job_config.events.inputs[].validate BOOL

    If :data:`True`, various checks will be done when source
    gets initialized (e.g., is table present?). This should be
    :data:`True` for most scenarios in order to catch errors as early as
    possible (pipeline construction instead of pipeline execution). It
    should be :data:`False` if the table is created during pipeline
    execution by a previous step. Defaults to :data:`True`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].coder STR

    A string representing the import path to a coder
    for the table rows if serialized to disk.

    If not specified, then the default coder is
    :class:`~apache_beam.io.gcp.bigquery_tools.RowAsDictJsonCoder`,
    which will interpret every line in a file as a JSON serialized
    dictionary. This argument needs a value only in special cases when
    returning table rows as dictionaries is not desirable.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].use_standard_sql BOOL

    Specifies whether to use BigQuery's standard SQL
    dialect for the query. The default value is :data:`False`.
    If set to :data:`True`, the query will use BigQuery's updated SQL
    dialect with improved standards compliance.
    This parameter is ignored for table inputs.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].flatten_results BOOL

    Flattens all nested and repeated fields in the query results.
    The default value is :data:`True`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].kms_key STR

    Optional Cloud KMS key name for use when creating new tables.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].skip_klio_read BOOL

    Inherited from :ref:`global event input config <skip-klio-read>`.

.. _event-input-config-files:

Text Files
^^^^^^^^^^

*Mode: batch*

Consuming KlioMessages from files hosted on `Google Cloud Storage`_ is supported for all runners.
Each line of a file is converted to :ref:`KlioMessage.data.element <data>`.

.. attention::

    Read from local files is only supported on Direct Runner.

Example configuration for reading local files:

.. code-block:: yaml

    name: my-cool-job
    job_config:
      events:
        inputs:
          - type: file
            location: ./input-ids.txt


Example configuration for reading files from Google Cloud Storage:


.. code-block:: yaml

    name: my-cool-job
    job_config:
      events:
        inputs:
          - type: file
            location: gs://my-event-input-bucket/input-ids.txt


.. option:: job_config.events.inputs[].type

    Value: ``file``

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.events.inputs[].location STR

    The file path to read from as a local file path or a GCS ``gs://`` path.
    The path can contain glob characters (``*``, ``?``, and ``[...]`` sets).

    .. attention::

        Local files are only supported for Direct Runner.

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.events.inputs[].min_bundle_size INT

    Minimum size of bundles that should be generated when splitting this source into bundles.
    See :class:`apache_beam.io.filebasedsource.FileBasedSource` for more details.

    Default is ``0``.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].compression_type STR

    Used to handle compressed input files.
    Typical value is :attr:`CompressionTypes.AUTO <apache_beam.io.filesystem.CompressionTypes.AUTO>`, in which case the underlying file_path's extension will be used to detect the compression.

    Default is :attr:`apache_beam.io.filesystem.CompressionTypes.AUTO`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].strip_trailing_newlines BOOL

    Indicates whether this source should remove the newline char in each line it reads before decoding that line.

    Default is ``True``.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].validate BOOL

    Flag to verify that the files exist during the pipeline creation time.

    Default is ``True``.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].skip_header_lines INT

    Number of header lines to skip.  Same number is skipped from each source file.
    Must be 0 or higher. Large number of skipped lines might impact performance.

    Default is ``0``.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].coder STR

    Coder used to decode each line.

    Defaults to :class:`apache_beam.coders.coders.StrUtf8Coder`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.inputs[].skip_klio_read BOOL

    Inherited from :ref:`global event input config <skip-klio-read>`.

Custom
^^^^^^

*Mode: streaming or batch*


Example configuration for a custom event input that is not supported by Klio:

.. code-block:: yaml

    name: my-cool-job
    job_config:
        events:
            inputs:
                - type: custom
                  some_key: some_value

.. option:: job_config.events.inputs[].type

    Value: ``custom``

    | **Runner**: Dataflow, Direct
    | *Required*


.. option:: job_config.events.inputs[].skip_klio_read BOOL

    Inherited from :ref:`global event input config <skip-klio-read>`. This will be set to ``True``
    automatically.

.. option:: job_config.events.inputs[].<custom-key> ANY

    Any arbitrary key-value pairs for custom event input configuration specific to a job.

Output
------


Google Pub/Sub
^^^^^^^^^^^^^^

*Mode: streaming*

Publishing KlioMessages to `Google Pub/Sub`_ is supported for all runners.

Example configuration for `Google Pub/Sub`_:

.. code-block:: yaml

    name: my-cool-job
    pipeline_options:
        streaming: True
    job_config:
        events:
        outputs:
            - type: pubsub
              topic: my-output-topic

.. option:: job_config.events.outputs[].type

    Value: ``pubsub``

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.events.outputs[].topic STR

    The topic that this job will publish to once it has finished processing. Unless
    ``skip_klio_write`` is ``True``, Klio will automatically write KlioMessages to this topic
    signifying work is completed.

    | **Runner**: Dataflow, Direct
    | *Required*


.. option:: job_config.events.outputs[].skip_klio_write BOOL

    Inherited from :ref:`global event output config <skip-klio-write>`.



.. _event-output-config-files:

Text Files
^^^^^^^^^^

*Mode: batch*

Writing KlioMessages to files hosted on `Google Cloud Storage`_ is supported for all runners.
Each line of a file is converted to :ref:`KlioMessage.data.element <data>`.

.. attention::

    Writing to local files is only supported on Direct Runner.

Example configuration for writing to local files:

.. code-block:: yaml

    name: my-cool-job
    job_config:
      events:
        outputs:
          - type: file
            location: ./output-ids
            file_name_suffix: .txt


Example configuration for writing files to Google Cloud Storage:


.. code-block:: yaml

    name: my-cool-job
    job_config:
      events:
        inputs:
          - type: file
            location: gs://my-event-input-bucket/output-ids
            file_name_suffix: .txt


.. option:: job_config.events.outputs[].type

    Value: ``file``

    | **Runner**: Dataflow, Direct
    | *Required*


.. _file_output_location_config:

.. option:: job_config.events.outputs[].location STR

    The file path to write to as a local file path or a GCS ``gs://`` path.
    The files written will begin with this location as a prefix, followed by a shard identifier (see |num_shards|_), and end in a common extension, if given by |file_name_suffix|_.
    In most cases, only this argument is specified and |num_shards|_,  |shard_name_template|_, and |file_name_suffix|_ use default values.


    .. attention::

        Local files are only supported for Direct Runner.

    | **Runner**: Dataflow, Direct
    | *Required*


.. _file_name_suffix_config:

.. option:: job_config.events.outputs[].file_name_suffix STR

    Suffix for the files written. Can be used to define desired file extension.

    Defaults to ``""``.

    | **Runner**: Dataflow, Direct
    | *Optional*


.. _append_trailing_newlines_config:

.. option:: job_config.events.outputs[].append_trailing_newlines BOOL

    Indicate whether this sink should write an additional newline char after writing each element.

    Defaults to ``True``.

    | **Runner**: Dataflow, Direct
    | *Optional*


.. _num_shards_config:

.. option:: job_config.events.outputs[].num_shards INT

    The number of files (shards) used for output.
    If not set, the runner will decide on the optimal number of shards.

    .. caution::

        Constraining the number of shards is likely to reduce the performance of a pipeline.
        Setting this value is not recommended unless you require a specific number of output files.

    | **Runner**: Dataflow, Direct
    | *Optional*


.. _shard_name_template_config:
.. option:: job_config.events.outputs[].shard_name_template STR

    A template string containing placeholders for the shard number and shard count.
    Currently only ``''`` and ``'-SSSSS-of-NNNNN'`` are supported patterns.
    When constructing a filename for a particular shard number, the upper-case letters ``S`` and ``N`` are replaced with the ``0``-padded shard number and shard count respectively.
    This argument can be ``''`` in which case it behaves as if |num_shards|_ was set to 1 and only one file will be generated.

    The default pattern used is ``'-SSSSS-of-NNNNN'``.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.outputs[].coder STR

    Coder used to encode each line.

    Defaults to :class:`apache_beam.coders.coders.ToBytesCoder`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.outputs[].compression_type STR

    Used to handle compressed output files. Typical value is :class:`apache_beam.io.filesystem.CompressionTypes.AUTO`, in which case the final file path's extension (as determined by |location|_, |file_name_suffix|_, |num_shards|_ and |shard_name_template|_) will be used to detect the compression.


    Defaults to :attr:`apache_beam.io.filesystem.CompressionTypes.AUTO`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.events.outputs[].header STR

    String to write at beginning of file as a header.
    If not ``None`` and |append_trailing_newlines|_ is set to ``True``, ``\n`` will be added.

    Defaults to ``None``.

    | **Runner**: Dataflow, Direct
    | *Optional*


.. option:: job_config.events.outputs[].footer STR

    String to write at the end of file as a footer.
    If not ``None`` and |append_trailing_newlines|_ is set to ``True``, ``\n`` will be added.

    Defaults to ``None``.

    | **Runner**: Dataflow, Direct
    | *Optional*


.. option:: job_config.events.inputs[].skip_klio_write BOOL

    Inherited from :ref:`global event output config <skip-klio-write>`.


Custom
^^^^^^

*Mode: streaming or batch*

Example configuration for a custom event input that is not supported by Klio:

.. code-block:: yaml

    name: my-cool-job
    job_config:
        events:
            outputs:
            - type: custom
              some_key: some_value

.. option:: job_config.events.outputs[].type

    Value: ``custom``
    | **Runner**: Dataflow, Direct
    | *Required*


.. option:: job_config.events.inputs[].skip_klio_write BOOL

    Inherited from :ref:`global event output config <skip-klio-write>`. This will be set to
    ``True`` automatically.


.. option:: job_config.events.outputs[].<custom-key> ANY

    Any arbitrary key-value pairs for custom event output configuration specific to a job.



.. _Google Pub/Sub: https://cloud.google.com/pubsub/docs
.. _Google BigQuery: https://cloud.google.com/bigquery/docs
.. _Google Cloud Storage: https://cloud.google.com/storage/docs
.. |num_shards| replace:: ``num_shards``
.. _num_shards: #num-shards-config
.. |file_name_suffix| replace::  ``file_name_suffix``
.. _file_name_suffix: #file-name-suffix-config
.. |shard_name_template| replace:: ``shard_name_template``
.. _shard_name_template: #shard-name-template-config
.. |append_trailing_newlines| replace:: ``append_trailing_newlines``
.. _append_trailing_newlines: #append-trailing-newlines-config
.. |location| replace:: ``location``
.. _location: #file-output-location-config

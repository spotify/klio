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


.. todo:: document supported batch event outputs


.. _Google Pub/Sub: https://cloud.google.com/pubsub/docs
.. _Google BigQuery: https://cloud.google.com/bigquery/docs


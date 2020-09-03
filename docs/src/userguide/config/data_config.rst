.. _data-config:

Data Configuration
==================

Input
-----

Google Cloud Storage
^^^^^^^^^^^^^^^^^^^^

Example configuration for `Google Cloud Storage`_:

    .. code-block:: yaml

        name: my-cool-job
        pipeline_options:
          streaming: True
        job_config:
          data:
            inputs:
              - type: gcs
                location: gs://my-bucket/my-jobs-folder
                file_suffix: ogg

.. _data-inputs-type:
.. option:: job_config.data.inputs[].type STR

    Value: ``gcs``

    | **Runner**: Dataflow, Direct
    | *Required*

.. _data-inputs-location:
.. option:: job_config.data.inputs[].location STR

    The GCS bucket of this job’s binary data input, usually an upstream job's output. Must be a
    valid Cloud Storage URL, beginning with ``gs://``.

    Required for Klio's automatic default existence checks.

    .. todo::

        Link to existence checks once documented.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. _data-inputs-file-suffix:
.. option:: job_config.data.inputs[].file_suffix STR

    The general file suffix or extension of input files.

    Required for Klio's automatic default existence checks.

    .. todo::

        Link to existence checks once documented.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.data.inputs[].skip_klio_existence_check BOOL

    Inherited from :ref:`global data input config <skip-input-ext-check>`.

.. option:: job_config.data.inputs[].ping BOOL

    Inherited from :ref:`global data input config <ping-mode>`.

Custom
^^^^^^

Example configuration for a custom data input that is not supported by Klio:

    .. code-block:: yaml

        name: my-cool-job
        job_config:
          data:
            inputs:
              - type: custom
                some_key: some_value

.. option:: job_config.data.inputs[].type

    Value: ``custom``

    | **Runner**: Dataflow, Direct
    | *Required*


.. option:: job_config.events.inputs[].skip_klio_existence_check BOOL

    Inherited from :ref:`global data input config <skip-input-ext-check>`. This will be set to
    ``True`` automatically.


.. option:: job_config.data.inputs[].ping BOOL

    Inherited from :ref:`global data input config <ping-mode>`.


.. option:: job_config.events.inputs[].<custom-key> ANY

    Any arbitrary key-value pairs for custom data input configuration specific to a job.


Output
------

Google Cloud Storage
^^^^^^^^^^^^^^^^^^^^

Example configuration for `Google Cloud Storage`_:

    .. code-block:: yaml

        name: my-cool-job
        pipeline_options:
          streaming: True
        job_config:
          data:
            outputs:
              - type: gcs
                location: gs://my-bucket/my-jobs-folder
                file_suffix: .wav

.. option:: job_config.data.outputs[].type STR

    Value: ``gcs``

    | **Runner**: Dataflow, Direct
    | *Required*

.. option:: job_config.data.outputs[].location STR

    The GCS bucket of this job’s binary data output. Must be a valid Cloud Storage URL, beginning
    with ``gs://``.

    Required for Klio's automatic default existence checks.

    .. todo::

        Link to existence checks once documented.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.data.outputs[].file_suffix STR

    The general file suffix or extension of input files.

    Required for Klio's automatic :ref:`default existence checks <data-existence-checks>`.

    | **Runner**: Dataflow, Direct
    | *Optional*

.. option:: job_config.data.outputs[].skip_klio_existence_check BOOL

    Inherited from :ref:`global data output config <skip-output-ext-check>`.

.. option:: job_config.data.outputs[].force BOOL

    Inherited from :ref:`global data output config <force-mode>`.


Custom
^^^^^^

Example configuration for a custom data output that is not supported by Klio:

    .. code-block:: yaml

        name: my-cool-job
        job_config:
          data:
            outputs:
              - type: custom
                some_key: some_value

.. option:: job_config.data.outputs[].type

    Value: ``custom``

    | **Runner**: Dataflow, Direct
    | *Required*


.. option:: job_config.events.outputs[].skip_klio_existence_check BOOL

    Inherited from :ref:`global data output config <skip-output-ext-check>`. This will be set to
    ``True`` automatically.


.. option:: job_config.data.outputs[].force BOOL

    Inherited from :ref:`global data output config <force-mode>`.


.. option:: job_config.events.outputs[].<custom-key> ANY

    Any arbitrary key-value pairs for custom data output configuration specific to a job.


.. _Google Cloud Storage: https://cloud.google.com/storage/docs

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


.. todo:: document supported batch event inputs


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


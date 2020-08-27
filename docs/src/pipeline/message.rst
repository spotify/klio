The Klio Message
================

The ``KlioMessage`` is protobuf data passed between transforms (and streaming jobs). Its data
represents one unit of work for a transform.

The ``KlioMessage`` is found in ``klio.proto`` and can be imported via
``klio_core.proto.klio_pb2``.

.. todo::

    Link to ``klio.proto`` file once the repo is public. Otherwise the doc build will fail.


.. _kliomessage:

``KlioMessage``
---------------

.. option:: metadata

    Metadata related to the message.

    | *Type:* :ref:`Metadata`
    | *Required*

.. option:: data

    Data of the message.

    | *Type:* :ref:`Data`
    | *Required*

.. option:: version

    Version of the message.

    | *Type:* :ref:`Version`
    | *Required*

.. _metadata:

``KlioMessage.Metadata``
^^^^^^^^^^^^^^^^^^^^^^^^

.. option:: downstream

    Jobs by which the message must be processed. If empty, then all jobs that receive the message
    will process it. If not empty, then the job will check if itself is listed within
    ``downstream``. If it's not, the message will be ignored and no work will be processed.

    *Deprecated.* Users should migrate to ``Metadata.intended_recipients``.

    | *type:* :ref:`KlioJob`
    | *repeated*


.. option:: visited

    Jobs by which the message has already been processed. No jobs are repeated. When a message is
    in ping mode (by setting ``ping`` to ``True``), this is used to log/visualize the DAG.

    | *type:* :ref:`KlioJob`
    | *repeated*

.. option:: job_audit_log

    Audit log for all jobs that the message has visited. This can be considered the audit trail
    for a message.

    | *type:* :ref:`auditlog`
    | *repeated*


.. option:: ping

    If ``True``, then no transformation work will be done for this message, and the message will
    be published to the job's output topic(s). The job will log about the received message. This
    is meant for debugging and/or visualizing the DAG.

    | *Type:* ``bool``
    | *Optional, default:* ``False``

.. option:: force

    If ``True``, and if the output data already exists for the message, then the job will force
    the transform to run again.

    | *Type:* ``bool``
    | *Optional, default:* ``False``

.. option:: intended_recipients

    Jobs by which the message must be processed. Used to detected between
    :ref:`top-down <top-down>` and :ref:`bottom-up <bottom-up>` execution modes.

    | *Type:* :ref:`recipients`
    | *Required* for v2


.. _recipients:

``KlioMessage.Metadata.Recipients``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

One of the following attributes are required:

.. option:: anyone

    Current message is intended for any recipient, signifying :ref:`top-down <top-down>`
    execution. Mutually exclusive with ``KlioMessage.Metadata.Recipients.limited``.

    | *Type:* :ref:`anyone`


.. option:: limited

    Current message is intended for the included recipients, signifying
    :ref:`bottom-up <bottom-up>` execution. Mutually exclusive with
    ``KlioMessage.Metadata.Recipients.anyone``.

    | *Type:* :ref:`limited`


.. _anyone:

``KlioMessage.Metadata.Recipients.Anyone``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is an empty "stub" message. Its presence is used to simply signify :ref:`top-down <top-down>`
execution.


.. _limited:

``KlioMessage.Metadata.Recipients.Limited``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. option:: recipients

    An array of KlioJobs. Only jobs included in ``recipients`` should process the message.
    Otherwise, the job should just drop the message to avoid further processing.

    | *Type:* :ref:`kliojob`
    | *Repeated*


.. option:: trigger_children_of

    When set to a particular job, it signifies that the message was *originally* in
    :ref:`top-down <top-down>` execution mode across a :doc:`graph <../anatomy/graph>` of jobs,
    but a dependency was missing for the job assigned to ``trigger_children_of``, therefore
    triggering :ref:`bottom-up <bottom-up>` execution for a subset of the graph. Once
    dependencies are made available, the job triggering bottom-up execution for that subset
    should then return the message to top-down mode. This is done by re-assigning
    ``KlioMessage.Metadata.intended_recipients`` to ``Anyone``.

    | *Type:* :ref:`kliojob`


.. _data:

``KlioMessage.Data``
^^^^^^^^^^^^^^^^^^^^

.. option:: element

    The reference identifier that refers to a particular file on which the job will perform work.

    | *Type:* ``bytes``
    | *Required*


.. option:: payload

    Data shared between transforms. It reflects what the previous transform in the pipeline
    returned/yielded (if that transform was decorated with the :ref:`handle-klio` decorator). The
    first transform in the pipeline after reading from event input will always be ``None``.

    See :doc:`transforms` for how to make use of a message's payload.

    | *Type:* ``bytes``
    | *Optional*


.. option:: entity_id

    The reference identifier that refers to a particular file on which the job will perform work.

    *Deprecated.* Users should migrate to ``data.element``.

    | *Type:* ``bytes``
    | *Required*


.. _kliojob:

``KlioJob``
-----------

.. warning::

    ``KlioJob`` will be undergoing API changes for v2 of Klio.


.. option:: job_name

    Name of job (as configured in ``klio-job.yaml::job_name``).

    | *Type:* ``string``
    | *Required*

.. option:: gcp_project

    GCP project of job (as configured in ``klio-job.yaml::pipeline_options.project``).

    | *Type*: ``string``
    | *Required for Dataflow*

.. option:: inputs

    The job's event & data input(s)

    *Marked for deprecation.*

    | *Type*: :ref:`job-input`.
    | *Repeated*


.. _job-input:

``KlioJob.JobInput``
^^^^^^^^^^^^^^^^^^^^

.. warning::

    ``KlioJob.JobInput`` has been marked for deprecation for v2.


.. option:: topic

    The job's Pub/Sub input topic.

    | *Type*: ``string``
    | *Required*


.. option:: subscription

    The job's Pub/Sub input subscription.

    | *Type*: ``string``
    | *Optional*


.. option:: data_location

    The job's Pub/Sub input location of input GCS data.

    | *Type*: ``string``
    | *Optional*


.. _auditlog:

``KlioJobAuditLogItem``
-----------------------

.. option:: timestamp

    Timestamp of when the audit log item was created.

    | *Type:* ``google.protobuf.Timestamp``
    | *Required*


.. option:: klio_job

    The ``KlioJob`` that is working on the message.

    | *Type:* :ref:`kliojob`
    | *Required*

.. _version:

``Version``
-----------

.. option:: UNKNOWN

    No version set.

.. option:: V1

    Version 1 of ``KlioMessage``.

.. option:: V2

    Version 2 of ``KlioMessage``.

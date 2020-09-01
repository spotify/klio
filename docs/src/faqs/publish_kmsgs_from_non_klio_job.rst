.. _non-klio-publish:

Can I publish KlioMessages from a non-Klio job to a Klio job?
=============================================================

The publisher of KlioMessages does not need to be a Klio job, nor in Python. It just needs to have
the :ref:`kliomessage` proto definition compiled for whatever language in which you plan on
publishing messages.


Step 1
^^^^^^

Construct the ``KlioMessage``:

.. code-block:: python

    from google.cloud import pubsub
    from klio_core.proto import klio_pb2

    klio_message = klio_pb2.KlioMessage()
    klio_message.data.element = b"s0me-f1le-1D"


.. _step-2:

Step 2
^^^^^^

Add the intended recipients to the ``KlioMessage`` depending if this message should be
executed in :ref:`top-down` or :ref:`bottom-up` mode.

If the desired execution mode is :ref:`top-down <top-down>`, update the recipients to ``anyone``:

.. code-block::

    klio_message.metadata.intended_recipients.anyone.SetInParent()

Or if the desired execusion mode is :ref:`bottom-up <bottom-up>`, update the recipients to include
the receiving job:

.. code-block::

    this_klio_job = klio_pb2.KlioJob()

    # Needs to match `klio-job.yaml::job_name`
    this_klio_job.job_name = "my-job-name"

    # If running on Dataflow, this needs to
    # match `klio-job.yaml::pipeline_options.project`
    this_klio_job.gcp_project = "my-gcp-project"

    klio_message.metadata.intended_recipients\
        .limited.recipients.extend([job])


.. _step-3:

Step 3
^^^^^^

Finally, serialize the KlioMessage into ``bytes`` and publish:

.. code-block:: python

    # serialize to a bytestring
    to_pubsub = klio_message.SerializeToString()

    # this would be what is in
    # `klio-job.yaml:job_config.events[].inputs[].topic`
    JOB_INPUT_TOPIC = "projects/$YOUR_PROJECT/topics/$YOUR_TOPIC"
    client = pubsub.PublisherClient()
    client.publish(topic=JOB_INPUT_TOPIC, data=to_pubsub)


.. todo::

    Add link in :ref:`non-klio-publish` and :ref:`custom-proto-msgs` above to proto definition
    in ``core/src/klio_core/proto/klio.proto`` once repo is public.

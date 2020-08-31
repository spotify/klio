.. _faqs:

FAQs
====

.. _non-klio-msgs:

Can I consume a non-``KlioMessage``?
-------------------------------------

Yes! This can be particularly helpful when the publisher of messages can not create messages using
the ``KlioMessage`` protobuf definition.

Just configure your job to accept non-KlioMessages by setting ``allow_non_klio_messages``
to ``True`` in the :ref:`job's configuration <allow-non-klio>`.

To publish a non-``KlioMessage`` via the :doc:`klio-cli <ecosystem/cli/index>`, use the
``--non-klio`` flag:

.. code-block:: console

    $ klio message publish --non-klio '{"some": "jsondata"}'

.. _non-klio-publish:

Can I publish KlioMessages from a non-Klio job to a Klio job?
-------------------------------------------------------------

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


.. _custom-proto-msgs:

Can I use my own protobuf definition for the ``KlioMessage``?
-------------------------------------------------------------

Custom protos are implicitly supported with some small manual/custom work required. There are
2 parts that will need changes or custom code.


Part 1: Create a Custom Publisher
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The process is very similar to :ref:`publishing a KlioMessage from a non-Klio job
<non-klio-publish>`.

Step 1
~~~~~~

First, construct & serialize the custom protobuf message:

.. code-block:: python

    from google.cloud import pubsub

    from klio_core.proto import klio_pb2
    # import your own protobuf def
    from my_proto import my_proto_pb2


    # Build your own message based off of your custom proto
    custom_proto_msg = my_proto_pb2.MyCustomProtoMessage()
    custom_proto_msg.data = "some relevant data"
    # Serialize to a bytestring
    custom_proto_msg_serialized = custom_proto_msg.SerializeToString()


Step 2
~~~~~~

Next, construct the KlioMessage with the serialized custom protobuf message.

.. code-block:: python

    klio_message = klio_pb2.KlioMessage()
    # Assign the custom proto data to either element OR payload.
    # Option 1: element
    # Use element when you DO NOT have an otherwise unique
    # identifier that refers to data to be processed.
    klio_message.data.element = custom_proto_msg_serialized

    # Option 2: payload
    # Use payload when DO have a unique identifier to refer
    # data to be processed.
    klio_message.data.payload = custom_proto_msg_serialized

Then follow :ref:`step-2` and :ref:`step-3` from :ref:`above <non-klio-publish>`.

.. attention::

    If the serialized custom protobuf message is assigned to ``klio_message.data.element`` as
    option 1 above outlines (instead of ``klio_message.data.payload``), then the default existence
    checks that Klio does for :ref:`input data <skip-input-ext-check>` and
    :ref:`output data <skip-output-ext-check>` will need to be turned off (and implement your own
    if they're needed).


Part 2: Update Transform Code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Jobs consuming custom protobuf messages will need to handle their deserialization:

.. code-block:: python

    # transforms.py
    import apache_beam as beam

    from klio.transforms import decorators

    # import your own protobuf def
    from my_proto import my_proto_pb2


    class YourTransform(beam.DoFn):
        @decorators.handle_klio
        def process(self, data):
            custom_msg = my_proto_pb2.YourCustomProtoMessage()
            # Deserialize from bytestring into custom proto
            # message object.
            # Option 1: deserialize from element
            custom_msg.ParseFromString(data.element)

            # Option 2: deserialize from payload
            custom_msg.ParseFromString(data.payload)

            # <-- rest of transform logic -->

            yield data


No configuration changes are needed.


.. attention::

    The above example yields the original ``data`` value that it received. If the job needs to
    pass :doc:`state <pipeline/state>` between transforms, and that state is a custom protobuf
    message, then be sure to re-serialize the ``custom_msg`` object to ``bytes``.



.. todo::

    Add link in :ref:`non-klio-publish` and :ref:`custom-proto-msgs` above to proto definition
    in ``core/src/klio_core/proto/klio.proto`` once repo is public.

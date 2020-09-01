
.. _custom-proto-msgs:

Can I use my own protobuf definition for the ``KlioMessage``?
=============================================================

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

Then follow :ref:`step-2` and :ref:`step-3` from :doc:`publish_kmsgs_from_non_klio_job`

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
    pass :doc:`state <../userguide/pipeline/state>` between transforms, and that state is a custom
    protobuf message, then be sure to re-serialize the ``custom_msg`` object to ``bytes``.



.. todo::

    Add link in :ref:`non-klio-publish` and :ref:`custom-proto-msgs` above to proto definition
    in ``core/src/klio_core/proto/klio.proto`` once repo is public.

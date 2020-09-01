.. _non-klio-msgs:

Can I consume a non-``KlioMessage``?
====================================

Yes! This can be particularly helpful when the publisher of messages can not create messages using
the ``KlioMessage`` protobuf definition.

Just configure your job to accept non-KlioMessages by setting ``allow_non_klio_messages``
to ``True`` in the :ref:`job's configuration <allow-non-klio>`.

To publish a non-``KlioMessage`` via the :doc:`klio-cli <../reference/cli/index>`, use the
``--non-klio`` flag:

.. code-block:: console

    $ klio message publish --non-klio '{"some": "jsondata"}'

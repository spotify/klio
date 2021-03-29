``klio.transforms`` Subpackage
==============================

.. toctree::
   :maxdepth: 1
   :hidden:

   decorators
   helpers
   io
   core

.. automodule:: klio.transforms

:doc:`decorators`
^^^^^^^^^^^^^^^^^

.. currentmodule:: klio.transforms.decorators

.. autosummary::
    :nosignatures:

    handle_klio
    timeout
    retry
    set_klio_context
    inject_klio_context
    serialize_klio_message
    profile


:doc:`helpers`
^^^^^^^^^^^^^^^^^

.. currentmodule:: klio.transforms.helpers

.. autosummary::
    :nosignatures:

    KlioMessageCounter
    KlioGcsCheckInputExists
    KlioGcsCheckOutputExists
    KlioFilterPing
    KlioFilterForce
    KlioWriteToEventOutput
    KlioDrop
    KlioCheckRecipients
    KlioUpdateAuditLog
    KlioDebugMessage
    KlioSetTrace


:doc:`io`
^^^^^^^^^

.. currentmodule:: klio.transforms.io

.. autosummary::
    :nosignatures:

    KlioReadFromText
    KlioReadFromBigQuery
    KlioReadFromAvro
    KlioReadFromPubSub
    KlioWriteToText
    KlioWriteToBigQuery
    KlioWriteToAvro
    KlioWriteToPubSub
    KlioMissingConfiguration


:doc:`core`
^^^^^^^^^^^


.. currentmodule:: klio.transforms.core

.. autosummary::
    :nosignatures:

    KlioContext


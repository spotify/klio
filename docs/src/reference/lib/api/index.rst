API
===

.. toctree::
   :maxdepth: 1
   :hidden:

   decorators
   helpers
   io
   core



Decorators
----------

.. currentmodule:: klio.transforms

.. autosummary::
    :nosignatures:

    decorators.handle_klio
    decorators.timeout
    decorators.set_klio_context
    decorators.inject_klio_context
    decorators.serialize_klio_message


Helper Transforms
-----------------

.. currentmodule:: klio.transforms

.. autosummary::
    :nosignatures:

    helpers.KlioGcsCheckInputExists
    helpers.KlioGcsCheckOutputExists
    helpers.KlioFilterPing
    helpers.KlioFilterForce
    helpers.KlioWriteToEventOutput
    helpers.KlioDrop
    helpers.KlioCheckRecipients
    helpers.KlioUpdateAuditLog
    helpers.KlioDebugMessage
    helpers.KlioSetTrace


I/O Transforms
--------------

.. currentmodule:: klio.transforms

.. autosummary::
    :nosignatures:

    io.KlioReadFromText
    io.KlioReadFromBigQuery
    io.KlioReadFromAvro
    io.KlioWriteToText
    io.KlioWriteToBigQuery
    io.KlioMissingConfiguration

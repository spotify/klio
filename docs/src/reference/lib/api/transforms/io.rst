I/O
===

.. currentmodule:: klio.transforms.io


.. autoclass:: KlioReadFromText()
    :inherited-members: apache_beam.io.ReadFromText.__init__
    :members: __init__
.. autoclass:: KlioReadFromBigQuery()
.. autoclass:: KlioReadFromAvro()
.. autoclass:: KlioReadFromPubSub()
.. autoclass:: KlioWriteToText()
    :inherited-members: apache_beam.io.textio.WriteToText.__init__
    :members: __init__
.. autoclass:: KlioWriteToBigQuery()
    :inherited-members: apache_beam.io.WriteToBigQuery.__init__
    :members: __init__
.. autoclass:: KlioWriteToAvro()
.. autoclass:: KlioWriteToPubSub()

.. autoexception::  KlioMissingConfiguration

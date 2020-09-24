.. _custom-ext-checks:

Custom Data Existence Checks
============================


Klio does basic input & output existence checks, but if your pipeline requires something more
complex, you will have to implement your own, and tell Klio to skip its default existence checks
(see :ref:`job configuration documentation <job-config>` for :ref:`input <skip-input-ext-check>`
and :ref:`output <skip-output-ext-check>` data existence checks).


.. _custom-in-check:

Custom Input Data Check
-----------------------

.. literalinclude:: ../../../../examples/snippets/custom_ext_checks.py
   :language: python
   :start-at: import
   :end-before: BonjourOutputCheck
   :caption: `examples/snippets/custom_ext_checks.py`_


.. tip::

    If you have a method that needs access to ``self._klio`` but does not handle a
    ``KlioMessage``, then use the :func:`@set_klio_context
    <klio.transforms.decorators.set_klio_context>` to attach :ref:`Klio context <kliocontext>` to
    the ``self`` instance.

    If you do so on the ``setup`` method, the ``self._klio`` context should then be
    available on all other methods once it's unpickled onto the Dataflow workers.

    You may also set it on the class's ``__init__`` method, but that makes the context only
    available on the driver (aka locally or on tingle when launching a job).


Custom Output Data Check
------------------------

This is very similar to the :ref:`example input check above <custom-in-check>` with the addition
of using :class:`Tagged Outputs <apache_beam.pvalue.TaggedOutput>`. Beam allows multiple outputs
from a single transform via "tagging". Here, we tag if output data has been ``found`` or
``not_found``. This is then used in ``run.py`` (:ref:`below <custom-ext-check-run>`), allowing
branches in logic for the pipeline.

.. literalinclude:: ../../../../examples/snippets/custom_ext_checks.py
   :language: python
   :pyobject: BonjourOutputCheck
   :caption: `examples/snippets/custom_ext_checks.py`_


.. _custom-ext-check-run:

Using Custom Existence Checks in your Pipeline
----------------------------------------------

In combination with the :class:`KlioFilterForce <klio.transforms.helpers.KlioFilterForce>` helper
transform, just add a few lines to string everything together.

.. admonition:: Notice the use of ``with_outputs()``
    :class: caution

    Since we used :class:`Tagged Outputs <apache_beam.pvalue.TaggedOutput>` in our
    ``BonjourOutputCheck`` class, we need to invoke our transform using the ``with_outputs`` method of to :class:`apache_beam.ParDo <apache_beam.transforms.core.ParDo>`.


.. literalinclude:: ../../../../examples/snippets/custom_ext_checks.py
   :language: python
   :start-at: from klio.transforms import helpers
   :caption: `examples/snippets/custom_ext_checks.py`_


The ``BonjourOutputCheck`` tags output as either ``found`` or ``not_found`` (via Tagged Outputs).
We then handle the ``found`` output by passing them through the :ref:`KlioFilterForce
<filter-force>` transform.

Then we flatten the ``not_found`` tagged output from ``BonjourOutputCheck`` and the
``process`` output from ``KlioFilterForce`` into one :class:`PCollection
<apache_beam.pvalue.PCollection>` for easier handling.

Finally, we pipe the flattened ``PCollection`` into our ``BonjourInputCheck`` transform. This only
yields back found data (and just logs when input data is not found). We can then pipe the
``to_process`` ``PCollection`` to the rest of our job's logic.


.. _examples/snippets/custom_ext_checks.py: https://github.com/spotify/klio/tree/master/examples/snippets/custom_ext_checks.py

Klio Core
=========

:release:`latest release`: |klio-core-version| (:doc:`What's new? <changelog>`)

.. include:: /.current_status.rst

.. include:: ../../../../core/README.rst
    :start-after: start-klio-core-intro


.. toctree::
   :maxdepth: 1
   :hidden:

   Config <api/config>
   Core Utils <api/utils>
   Dataflow Client <api/dataflow>
   Exceptions <api/exceptions>
   api/variables
   changelog

----

:doc:`api/config`
-----------------

.. automodule:: klio_core.config
   :noindex:

.. currentmodule:: klio_core.config

.. autosummary::
    :nosignatures:

    KlioConfig
    KlioJobConfig
    KlioPipelineConfig

:doc:`api/utils`
----------------

.. automodule:: klio_core.options
   :noindex:

.. currentmodule:: klio_core.options

.. autosummary::

    image_tag
    direct_runner
    update
    show_logs
    inteval
    include_children
    multiprocess
    plot_graph
    maximum
    per_element
    iterations


:doc:`api/utils`
----------------

.. automodule:: klio_core.utils
   :noindex:

.. currentmodule:: klio_core.utils

.. autosummary::

    get_publisher
    get_or_initialize_global
    set_global
    get_global
    delete_global


:doc:`api/dataflow`
-------------------

.. automodule:: klio_core.dataflow
   :noindex:

.. currentmodule:: klio_core.dataflow

.. autosummary::
    :nosignatures:

    DataflowClient
    get_dataflow_client


:doc:`api/variables`
--------------------

.. automodule:: klio_core.variables
   :noindex:

.. currentmodule:: klio_core.variables

.. autosummary::
    :nosignatures:

    DATAFLOW_REGIONS



:doc:`api/exceptions`
---------------------------

.. automodule:: klio_core.exceptions
   :noindex:

.. currentmodule:: klio_core.exceptions

.. autosummary::

    KlioConfigTemplatingException

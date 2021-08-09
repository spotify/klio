Changelog
=========

.. _core-21.8.0:

21.8.0 (UNRELEASED)
-------------------

.. start-21.8.0

Added
*****

* Add ``KlioRunner.DIRECT_GKE_RUNNER`` to variables to support ``DirectGKERunner``

.. end-21.8.0

.. _core-21.2.0:

21.2.0 (2021-03-16)
-------------------

.. start-21.2.0

Changed
*******

* Moved ``IndentListDumper`` to ``klio_core`` config utils.

.. end-21.2.0

0.2.2 (2021-02-11)
------------------

Fixed
*****

* Fixed handling of configured BQ schemas when defined as a string (thank you `gfalcone <https://github.com/spotify/klio/pull/165>`_!).
* Fixed config bug that skipped preprocessing (overrides, templates) of dict parsed from YAML
* ``KlioWriteToAvro`` has been enabled as an output event type (previously missing).


0.2.1 (2020-12-03)
------------------

* Common klio-cli and exec options moved to klio-core
* ``with_klio_config`` moved from klio-cli to klio-core

0.2.0 (2020-10-02)
------------------

Initial public release!

CLI Changelog
=============

21.8.0 (UNRELEASED)
-------------------

Added
*****

* Safety check built image vs deployed image differences when deploying with `DirectGKERunner`

21.7.0 (UNRELEASED)
-------------------

Fixed
*****

* Fixed bug with ``klio message publish`` not working on ``google-cloud-pubsub > 2.3.0``

Added
*****

* Support for deploying, stopping, and deleting jobs with `DirectGKERunner`

.. _cli-21.2.0:

21.2.0 (2021-03-16)
-------------------

.. start-21.2.0

Fixed
*****

* The ``--config-file`` flag can now be used in ``setup.py`` projects.

Changed
*******

* Moved ``IndentListDumper`` to ``klio_core`` config utils.
* Runtime config file for ``klioexec`` now written to ``klio-job-run-effective.yaml`` in the job's directory.

.. end-21.2.0

1.0.5 (2021-01-26)
------------------

Fixed
*****
* Fixed a runtime bug in ``klio job config`` commands
* Fixed language with ``klio job create`` command to make it more general to job type
* Fixed bug where ``klio job create`` in batch mode tried to create topics and subscriptions
* Fixed template for new jobs to correctly include the ``klio-job.yaml`` file
* Fixed syntax for ``MANIFEST.in`` template

Changed
*******

* Updated Beam SDK version to latest known working version

1.0.4 (2021-01-04)
------------------

Fixed
*****
Fixes "klioexec: error: unrecognized arguments: --config-file" error in
test command.

1.0.3 (2020-12-16)
------------------

Fixed
*****
Fixes "Invalid constructor input" errors in verify, delete, and message commands.


1.0.2 (2020-12-03)
------------------

* Common cli and exec options moved to klio-core
* Materializes config file including overrides to hand off to ``klioexec run``


1.0.0 (2020-10-02)
------------------

Initial public release!

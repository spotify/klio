CLI Changelog
=============

.. _cli-21.10.0:

21.10.0 (UNRELEASED)
--------------------

.. start-21.10.0

Added
*****

* Error out when a user tries to run a Dataflow-based job with Stackdriver log-based metrics client configured.

Fixed
*****

* Correctly validate existence of Dataflow-related Klio config when running on Dataflow (and not just "not --direct-runner").

Changed
*******

* When running a job, effective config is no longer written to ``klio-job-run-effective.yaml``, but instead to a temp file.  This file no longer needs to be included in ``setup.py`` projects (See `PR 233 <https://github.com/spotify/klio/pull/233>`_).

.. end-21.10.0

.. _cli-21.9.0:

21.9.0 (2021-10-12)
-------------------

.. start-21.9.0

Added
*****

* Support for applying and deleting all resource (e.g., deployment, hpa, vpa)
    types configured in yaml files in a job's kubernetes directory.

.. end-21.9.0

.. _cli-21.8.0:

21.8.0 (2021-09-03)
-------------------

.. start-21.8.0

Added
*****

* Support for deploying, stopping, and deleting jobs with ``DirectGKERunner``
* Support for setting required and best-practices labels for Kubernetes
    deployments as well as user-set labels
* Enable job test command to use the ``--config-file`` option

Fixed
*****

* Fixed bug with writing of ``klio-job-run-effective.yaml`` for ``klio job profile`` commands
* Fixed bug with ``klio message publish`` not working on ``google-cloud-pubsub > 2.3.0``


.. end-21.8.0


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

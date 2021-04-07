Changelog
=========

21.4.0rc1 (UNRELEASED)
----------------------

Changed
*******

* Use newly-added ``KlioReadFromPubSub`` and ``KlioWriteToPubSub`` transforms instead of native Beam's transforms.

Fixed
*****

* Limit version of ``line_profiler`` as the latest introduced breaking API changes.

21.3.0 (UNRELEASED)
-------------------

*  Delete me: placeholder for when ``21.3.0`` is merged in.

.. _exec-21.2.0:

21.2.0 (2021-03-16)
-------------------

.. start-21.2.0

Fixed
*****

* ``klioexec`` now writes runtime config to include in ``setup.py`` distribution.


.. end-21.2.0

0.2.2 (2021-01-14)
------------------

Fixed
*****

* Reintroduced comparing build to runtime config

0.2.1 (2020-12-03)
------------------

* Common cli and exec options moved to klio-core
* Config pickling - ``RunConfig`` is used to set a main-session global variable
* Removed worker dependency on effective-klio-job.yaml
* Add warning if FnApi is used in a batch job

0.2.0 (2020-10-02)
------------------

Initial public release!

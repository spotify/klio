Changelog
=========

0.2.3 (UNRELEASED)
------------------

Fixed
*****

* ``klioexec`` now writes runtime config to include in ``setup.py`` distribution.


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

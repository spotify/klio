Changelog
=========

21.3.0rc1 (UNRELEASED)
----------------------

Added
*****

* Add support for using Beam's metrics API directly.

Changed
*******

* The Beam metrics client will always be used, no matter the configured runner.
* Marked Klio's support for Stackdriver log-based metrics for deprecation and eventual removal.

0.2.4 (2021-01-14)
------------------

Added
*****

* Add thread limiting context manager utility (See `KEP 2: Thread Management <https://docs.klio.io/en/latest/keps/kep-002.html>`_).
* Add default thread management to ``@handle_klio`` decorator (See `KEP 2: Thread Management <https://docs.klio.io/en/latest/keps/kep-002.html>`_).

Fixed
*****

* Partially reverted back to reading config from ``effective-klio-job.yaml`` (See `PR 147 <https://github.com/spotify/klio/pull/147>`_).


0.2.3 (2021-01-04)
------------------

Fixed
*****

* Fixed non-Klio -> Klio message parsing.
* Fixed calling of ``to_klio_message`` in helper transforms.


0.2.2 (2020-12-11)
------------------

Added
*****

* Added support for writing an avro file via ``KlioWriteToAvro``


Fixed
*****

* Allow for support of empty ``job_config.data`` values for the built-in helper filter transforms.


0.2.1.post2 (2020-12-03)
------------------------

Fixed
*****

* Requires klio-core>=0.2.1 now that dependent changes have been released in new core version


0.2.1.post1 (2020-11-30)
------------------------

Fixed
*****

* Requires klio-core<0.2.1,>=0.2.0 to prevent useage of 0.2.1 until dependent code is released
* Klio lib requires changes not yet released in klio-core

0.2.1 (2020-11-24)
------------------------

Fixed
*****

* Handling of exceptions yielded by functions/methods decorated with @handle_klio
* KlioReadFromBigQuery rewritten as reader + map transform

0.2.0.post1 (2020-11-02)
------------------------

Fixed
*****

* Limited apache beam dependency to <2.25.0 due to a breaking change

0.2.0 (2020-10-02)
------------------

Initial public release!

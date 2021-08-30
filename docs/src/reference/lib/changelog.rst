Changelog
=========

.. _lib-21.8.0:

21.8.0 (UNRELEASED)
-------------------

.. start-21.8.0

Added
*****

* Add ``KlioReadFromPubSub`` and ``KlioWriteToPubSub`` IO transforms.
* Add default metrics to be collected in Klio's IO transforms.
* Add default metrics to be collected in Klio's helper transforms.
* Add default metrics to be collected in Klio's decorators.
* Add support for using Beam's metrics API directly.
* Add DirectGKERunner, runs direct runner on GKE with added logic to only ack pub/sub message when:
  * the pipeline successfully ran, but before any write to event output happens if any,
  * the message is skipped because output data already exists,
  * the message is dropped because input data does not exist, or
  * the message is dropped because it was not the intended recipient.

* Add metrics interface for shumway

Fixed
*****

* ``KlioTriggerUpstream`` no longer raises a pickling error when trying to log.

Changed
*******

* The Beam metrics client will always be used, no matter the configured runner.
* Marked Klio's support for Stackdriver log-based metrics for deprecation and eventual removal.

.. end-21.8.0


.. _lib-21.2.0:

21.2.0 (2021-03-16)
-------------------

.. start-21.2.0

Changed
*******

* Changed thread limiter logging to debug.
* Workers will look for ``klio-job-run-effective.yaml`` before dropping back to ``.effective-klio-job.yaml``.

.. end-21.2.0

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

* Requires klio-core<0.2.1,>=0.2.0 to prevent usage of 0.2.1 until dependent code is released
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

* Limited Apache beam dependency to <2.25.0 due to a breaking change

0.2.0 (2020-10-02)
------------------

Initial public release!

Changelog
=========

0.2.1.post (2020-11-30)
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

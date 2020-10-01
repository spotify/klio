Klio CLI
========

:release:`latest release`: |klio-cli-version| (:doc:`What's new? <changelog>`)

.. include:: ../../../../cli/README.rst
    :start-after: start-klio-cli-intro



.. toctree::
   :maxdepth: 1
   :hidden:

   klio image Command <api/cli/image>
   klio job Command <api/cli/job>
   klio message Command <api/cli/message>
   changelog

----

``klio``
--------

.. automodule:: klio_cli.cli


.. code-block:: console

    klio [OPTIONS] COMMAND [ARGS]...


.. rubric:: Options

.. option:: --version

    Show the version and exit

.. option:: --help

    Show this message and exit.


.. rubric:: Commands


|image|_
    Manage a job’s Docker image.

|job|_
    Create and manage Klio jobs.

|message|_
    Manage a job’s message queue via Pub/Sub.



.. |image| replace:: ``image``
.. _image: ./image.html
.. |job| replace:: ``job``
.. _job: ./job.html
.. |message| replace:: ``message``
.. _message: ./message.html

CLI
===

.. toctree::
   :maxdepth: 1
   :hidden:

   job
   message
   image


.. automodule:: klio_cli.cli


.. code-block:: console

    klio [OPTIONS] COMMAND [ARGS]...


.. rubric:: Options

.. option:: --version

    Show the version and exit


Sub-commands
------------

===========  =============================================
|image|_     Manage a job’s Docker image.
|job|_       Create and manage Klio jobs.
|message|_   Manage a job’s message queue via Pub/Sub.
===========  =============================================


.. |image| replace:: ``image``
.. _image: ./image.html
.. |job| replace:: ``job``
.. _job: ./job.html
.. |message| replace:: ``message``
.. _message: ./message.html

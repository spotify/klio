Installation
============

Requirements
------------

* `git`_
* Python version 3.5, 3.6, 3.7, or 3.8 (recommended installation via `pyenv`_)
* `Docker`_
* Google Cloud SDK (:ref:`setup instructions below <gcloud-setup>`)
* `pipx`_ (suggested) or `virtualenv`_ (`pyenv-virtualenv`_ when using `pyenv`_).

.. _gcloud-setup:

Google Cloud SDK Setup
^^^^^^^^^^^^^^^^^^^^^^

.. attention::

    A Klio streaming job is :violetemph:`dependent` on `Google Cloud Pub/Sub`_ and `Google Cloud Storage`_ (GCS).

    To create the job's relevant dependencies, you need to set up an account with Google Cloud Platform, create a project and enable those services for said project.

    You can begin that process `here <https://cloud.google.com/gcp/>`_.

Once you have set up your account on Google Cloud Platform (GCP), you will need to install and setup the `Google Cloud SDK`_ (a.k.a. ``gcloud``).

Follow the |recommended install instructions|_.
Once it has installed, set up the Google Cloud SDK by running:

.. code-block:: sh

    $ gcloud init

Next you will be asked to choose the :violetemph:`account` and :violetemph:`project ID` you would like to use.
Enter in the account you used to set up GCP and the project you created which is enabled with Google Cloud Pub/Sub and Google Cloud Storage.

Lastly, :violetemph:`authorize` Google Cloud for application **and** Docker access by running:

.. code-block:: sh

    $ gcloud auth application-default login
    $ gcloud auth configure-docker


Install ``klio-cli``
--------------------

Option 1: ``pipx``
^^^^^^^^^^^^^^^^^^

.. note::

    ``pipx`` is the recommended approach to installation the |klio-cli|_.

Install via `pipx`_:

.. code-block:: sh

    # create a project directory
    $ mkdir klio_quickstart && cd klio_quickstart

    # install klio-cli
    $ pipx install klio-cli

    # confirm installation
    $ klio --version

``pipx`` installs ``klio-cli`` into the local user space within its own virtualenv, so it's accessible no matter the current working directory or if a ``virtualenv`` is activated.

.. _install-option-2:

Option 2: ``virtualenv`` + ``pip``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. attention::

    If you use `pyenv`_, it's highly recommended to then use `pyenv-virtualenv`_. Once installed, follow :ref:`option 3 <install-option-3>` instead.

First, setup a new ``virtualenv``:

.. code-block:: sh

    # create a project directory
    $ mkdir klio_quickstart && cd klio_quickstart

    # create a new virtualenv within new project directory
    $ virtualenv klio-cli

    # activate the new virtualenv
    $ source klio-cli/bin/activate
    (klio-cli) $


Then install |klio-cli|_:

.. code-block:: sh

    # within the activate virtualenv
    (klio-cli) $ pip install klio-cli
    # confirm installation
    (klio-cli) $ klio --version

.. _install-option-3:

Option 3: ``pyenv-virtualenv`` + ``pip``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

First, setup a new ``virtualenv`` with ``pyenv-virtualenv``:

.. code-block:: sh

    # create a project directory
    $ mkdir klio_quickstart && cd klio_quickstart

    # create a new virtualenv called klio-cli
    $ pyenv virtualenv klio-cli

    # activate the new virtualenv
    $ pyenv activate klio-cli
    (klio-cli) $


Then install |klio-cli|_:

.. code-block:: sh

    # within the activate virtualenv
    (klio-cli) $ pip install klio-cli
    # confirm installation
    (klio-cli) $ klio --version


.. _git: https://git-scm.com/book/en/v2/Getting-Started-Installing-Git
.. _pyenv: https://github.com/pyenv/pyenv
.. _Docker: https://docs.docker.com/engine/install/
.. _pipx:  https://pypi.org/project/pipx/
.. _virtualenv: https://virtualenv.pypa.io/en/latest/
.. _pyenv-virtualenv: https://github.com/pyenv/pyenv-virtualenv
.. _Google Cloud Pub/Sub: https://cloud.google.com/pubsub/docs/overview
.. _Google Cloud Storage: https://cloud.google.com/storage/docs
.. _Google Cloud SDK: https://cloud.google.com/sdk

.. there's no way to do nested formatting within the prose, so we have to do it this way
    https://docutils.sourceforge.io/FAQ.html#is-nested-inline-markup-possible

.. |recommended install instructions| replace:: **recommended install instructions**
.. _recommended install instructions: https://cloud.google.com/sdk/docs/install
.. |klio-cli| replace:: ``klio-cli``
.. _klio-cli: https://pypi.org/project/klio-cli

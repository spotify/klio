Installation
============

.. include:: /.current_status.rst

Requirements
------------

* `git`_
* Python version 3.7, or 3.8 (recommended installation via `pyenv`_)
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


.. admonition:: See an error while installing?
    :class: hint

    When running ``pipx install klio-cli``, you may come across `a dependency mismatch error <https://github.com/spotify/klio/issues/113>`_, something like this:

    .. code-block:: console

        ERROR: After October 2020 you may experience errors when installing or updating packages. This is because pip will change the way that it resolves dependency conflicts.

        We recommend you use --use-feature=2020-resolver to test your packages with the new resolver before it becomes the default.

        google-api-python-client 1.12.5 requires google-api-core<2dev,>=1.21.0, but you'll have google-api-core 1.20.1 which is incompatible.
        klio-core 0.2.0 requires google-api-python-client<1.12,>=1.10.0, but you'll have google-api-python-client 1.12.5 which is incompatible.
        Could not find package klio-cli. Is the name correct?

    or like this:

    .. code-block:: console

        ERROR: After October 2020 you may experience errors when installing or updating packages. This is because pip will change the way that it resolves dependency conflicts.

        We recommend you use --use-feature=2020-resolver to test your packages with the new resolver before it becomes the default.

        google-api-python-client 1.12.5 requires google-api-core<2dev,>=1.21.0, but you'll have google-api-core 1.20.1 which is incompatible.
        klio-core 0.2.0 requires google-api-python-client<1.12,>=1.10.0, but you'll have google-api-python-client 1.12.5 which is incompatible.
        Internal error with venv metadata inspection.

        Unable to install klio-cli.
        Check the name or spec for errors, and verify that it can be installed with pip.

    **Fix:** Try re-running the command with the added argument:

    .. code-block:: sh

        pipx install klio-cli --pip-args="--use-feature=2020-resolver"


    **Still failing to install?** Please let us know by `filing an issue <https://github.com/spotify/klio/issues/new>`_.

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

    # update pip & setuptools within the virtualenv
    (klio-cli) $ pip install --upgrade pip setuptools


Then install |klio-cli|_:

.. code-block:: sh

    # within the activate virtualenv
    (klio-cli) $ pip install klio-cli --use-feature=2020-resolver
    # confirm installation
    (klio-cli) $ klio --version


.. collapsible:: Why the flag "``--feature=2020-resolver``"?

    The ``klio-cli`` libraries on which it depends have their own dependencies that can sometimes have conflicting versions.
    The Klio dev team has found that the new ``pip`` `resolver <https://discuss.python.org/t/announcement-pip-20-2-release/4863>`_ (as of version ``20.2``) can help fix those conflicts.

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

    # update pip & setuptools within the virtualenv
    (klio-cli) $ pip install --upgrade pip setuptools


Then install |klio-cli|_:

.. code-block:: sh

    # within the activate virtualenv
    (klio-cli) $ pip install klio-cli --use-feature=2020-resolver
    # confirm installation
    (klio-cli) $ klio --version


.. collapsible:: Why the flag "``--feature=2020-resolver``"?

    The ``klio-cli`` libraries on which it depends have their own dependencies that can sometimes have conflicting versions.
    The Klio dev team has found that the new ``pip`` `resolver <https://discuss.python.org/t/announcement-pip-20-2-release/4863>`_ (as of version ``20.2``) can help fix those conflicts.


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

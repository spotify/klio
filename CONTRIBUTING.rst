Contributing to the ``klio`` ecosystem
======================================

Requirements
------------

* Python 3.7, and 3.8 (suggestion: use `pyenv <https://github.com/pyenv/pyenv>`_ to install required versions)
* `tox <https://tox.readthedocs.io/en/latest/>`_ (suggestion: use `pipx <https://pypi.org/project/pipx/>`_ to install ``tox``)
* `protoc <https://github.com/protocolbuffers/protobuf/>`_

Project Overview
----------------

Each directory is its own Python package (except for ``docs``, ``examples``, and ``integration``):

.. code-block:: sh

    ├── audio/
    ├── cli/
    ├── core/
    ├── devtools/
    ├── docs/
    ├── examples/
    ├── exec/
    ├── integration/
    ├── lib/
    └── microsite/

The ``klio-cli`` (user-facing) commands essentially wrap ``klio-exec`` (non-user-facing) commands.
``klio-cli $CMD`` sets up the run context needed to correctly run ``klio-exec $CMD`` inside the
job's Docker container (i.e. mounts the job directory, sets up env vars, copies over credentials).
In general, changes to ``klio-cli`` require a corresponding change in ``klio-exec``, but there are
a few ``klio-cli`` commands self-contained and therefore do not need any changes to ``klio-exec`` (i.e. ``klio message publish``).

``klio-core`` is a non-user-facing library of common utilities, including protobuf definitions.
The ``klio`` library contains the base transform for users to implement, and the main message
processing logic (i.e. triggering parents, skipping already processed work, etc). ``klio-devtools``
is just a collection of utilities to help aid the development of klio.


Run Tests
---------

Tests can be invoked in two ways: ``pytest`` and ``tox``.


Run tests via ``tox``
^^^^^^^^^^^^^^^^^^^^^

This is the recommended way to run tests since tox will manage its own virtualenvs and is also
what will be run as part of the CI checks.

`tox <https://tox.readthedocs.io/en/latest/>`_ is a tool to easily run tests and do other
development tasks in a Python project, kind of like a Makefile for Python.  It is configured with
a ``tox.ini`` file in the root folder of a project.  Because Klio is divided into several Python
projects each one has its own ``tox.ini``.

.. caution::

    Occasionally tox may cache an outdated version of a dependency.
    If you find yourself getting errors that aren't reproducible by others or by CI, try deleting the ``.tox`` folder in your project's directory.

Before you begin, be sure your shell is **not** using a virtualenv.

First, install ``tox``:

.. code-block:: sh

    $ pipx install tox

Now you can navigate to one of the Klio sub-project directories and use ``tox`` to run unit
tests.  Currently ``tox`` is configured to run all tests using several different Python versions.
This can take a while, so it's much faster when testing to just run with one environment:

.. code-block:: sh

    $ tox -e py37

This might take a while the first time as ``tox`` sets up its envs and downloads dependencies, but
after that it should be pretty fast.

To run a specific test:

.. code-block:: sh

    $ tox -e py37 -- tests/unit/test_message_handler.py::test_preprocess_klio_message_non_klio


You can also use

.. code-block:: sh

    $ tox -e format

to run ``black`` and auto-format your code, and

.. code-block:: sh

    $ tox -e lint

to run ``flake8`` which enforces a number of syntactic and formatting conventions.  The CI will
run both of these (with ``black`` in non-edit mode), so be sure to run these yourself!


Run tests via ``pytest`` and ``pyenv`` for Multi-project Dev
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The above testing with ``tox`` will not work if you need to makes changes across several projects
at once, instead you will need to create your own virtualenv and install your checked-out code as
a local dependency.  Then you'll bypass ``tox`` and run ``pytest`` directly.

Ensure that you have followed the above instructions and installed `pyenv
<https://github.com/pyenv/pyenv>`_.  You will also need to install `pyenv-virtualenv
<https://github.com/pyenv/pyenv-virtualenv>`_.  Now we'll use ``pyenv`` to create a new virtualenv called ``klio-dev`` based on Python 3.7.7:

.. code-block:: sh

    $ pyenv virtualenv 3.7.7 klio-dev

We then have to activate it in order to use it:

.. code-block:: sh

    $ pyenv activate klio-dev

Your console's prompt should now include ``(klio-dev)`` to help you keep track of which venv
you're currently using.  Now we will install your local ``klio-core`` as a library:

.. code-block:: sh

    $ cd <klio-core-directory>
    $ pip install -e ".[dev]"

Next, navigate to the ``klio-cli`` dir and do the same thing:

.. code-block:: sh

    $ cd <klio-cli-directory>
    $ pip install -e ".[dev]"

Now you can run ``pytest`` directly (do _not_ run ``tox``)

.. code-block:: sh

    # in klio-cli directory
    $ pytest

You can now make changes in either project and they will be picked up immediately, no need to
re-install each time.

If you want to try running the ``klio-cli`` command directly yourself, you will have to run one
more command:

.. code-block:: sh

    $ pyenv rehash

This is necessary to make sure your terminal is pointing to the local version.


Integration Testing with ``klio-devtools``
------------------------------------------

The easiest way we've found to do local integration testing of changes to any Klio library *before*
making PRs is:

1. Identify or create a simple job that you can use to test your changes locally.

2. Temporarily update the package versions locally for ``klio-exec``, ``klio-core``, and ``klio`` libraries (found in their top-level ``__init__.py``). A helpful convention is to bump the patch version and add a suffix of ``.devN``. For instance, if the current version is ``1.2.3``, update it to ``1.2.4.dev1``. This is to ensure the correct versions are installed, and not reusing an already-released version. Once you're satisfied with your changes, undo these changes then run ``bumpversion <part>`` as usual.

3. Make sure the virtualenv for the ``klio-cli`` is activated, and that the ``klio-devtools`` package is installed (usually installed via ``pip install -e ".[dev]"``).

4. In the directory of your simple job, run ``klio-dev develop --klio-path $PATH_TO_REPO``. This will launch you into the job's container with ``klio``, ``klio-core``, and ``klio-exec`` installed as an editable Python package, and will pick up on any changes you make locally.

.. note::

    When using ``klio-dev develop``, the path to the root of the Klio repo can be either
    relative or absolute.

5. Now that you're inside the job's container, you can run ``klioexec $CMD`` or whatever else is needed to do any manual integration testing.


Protobuf
--------

``klio`` uses protobuf for messages between transforms via Google Pub/Sub. The protobuf definition
is located in the ``klio-core`` `library <https://github.com/spotify/klio/tree/master/core/src/klio_core/proto>`_.


Compile Protobuf
^^^^^^^^^^^^^^^^

.. admonition:: Attention!
    :class: caution

    If creating a new protobuf version, be sure to create a new directory and update the command/
    tox config below.

Manually compile protobuf:

.. code-block:: sh

    # Within top-level of repo
    $ protoc \
        --proto_path src/klio/proto/v1beta1 klio.proto \
        --python_out src/klio/proto/v1beta1


Or via ``tox``:

.. code-block:: sh

    # outside a virtualenv
    $ tox -e protoc

Documentation
-------------

All documentation related to the Klio ecosystem is in the top-level ``docs/`` directory/

Generate Documentation
^^^^^^^^^^^^^^^^^^^^^^

Create a new virtual environment for documentation & install dependencies:

.. code-block:: sh

    $ pyenv virtualenv 3.7.7 klio-docs
    $ pyenv activate klio-docs
    (klio-docs) $ cd docs
    (klio-docs) $ pip install -r requirements.txt -r klio-requirements.txt


.. note::

    You may see some errors of version conflicts when installing documentation dependencies.
    However, these should be benign and should not get in the way of generating documentation.


Once the environment is setup, documentation can be generated and viewed via ``make``:

.. code-block:: sh

    # in the docs/ directory
    (klio-docs) $ make clean && make html

To view them locally

.. code-block:: sh

    # in the docs/ directory
    (klio-docs) $ make clean && make livehtml

Then navigate to ``http://localhost:8888`` in your browser.


Changelog
---------

If your change is noteworthy, there needs to be a changelog entry so our users can learn about it!

* For each pull request, add a line in ``docs/src/reference/<package>/changelog.rst`` under the appropriate section (``Added``, ``Fixed``, ``Removed``, ``Changed``, or ``Dependencies Updated``) for the latest unreleased version (e.g. ``1.2.3 (UNRELEASED)``).

  * If there is no ``(UNRELEASED)`` version, start a new version section with a second-level header of ``1.2.3 (UNRELEASED)`` where ``1.2.3`` is the next anticipated release.
  * Include links to any relevant issues, KEPs, or PRs (particularly in the case of reverts).

* Wrap symbols like modules, functions, or classes into double backticks so they are rendered in a ``monospace font``.
* If you mention functions or other callables, add parentheses at the end of their names: ``klio.func()`` or ``klio.Class.method()``. This makes the changelog a lot more readable.
* Prefer simple past tense or constructions with "now". For example:

  * Added ``klio.fun_functions.func()``.
  * ``klio.func()`` now doesn't crash the Large Hadron Collider anymore when passed the *foobar* argument.

Refer to the `changelog format <https://github.com/spotify/klio/blob/master/RELEASING.rst#changelog-format>`_ in ``RELEASING.rst`` for a complete example, and `Update Changelog <https://github.com/spotify/klio/blob/master/RELEASING.rst#update-changelog>`_ when preparing the changelog for a new release.

Microsite
---------

The Klio `microsite/landing page <https://klio.io>`_ is a simple static site that uses `Bootstrap 4.5 <https://getbootstrap.com/docs/4.5/getting-started/introduction/>`_.
For now, it is not managed by any tooling (npm, gulp, etc); HTML, CSS, etc is maintained by hand.


Local Development
^^^^^^^^^^^^^^^^^

1. In ``microsite/index.html``, add ``<script src="js/reload.min.js"></script>`` to the bottom after the ``</body>`` closing tag.

.. caution::

    Be sure to remove this addition when committing changes.

2. Run the following to view the site locally:

.. code-block:: sh

    # in the microsite/ directory
    $ no virtualenv needed
    $ python -m http.server 8888

3. Navigate to ``http://localhost:8888`` in your browser. Any change made to files in the ``microsite/`` should be automatically reloaded (although CSS changes *may* need a manual browser refresh).


Deployment
^^^^^^^^^^

Deployment is setup **automatically** with a `designated workflow <https://github.com/spotify/klio/blob/master/.github/workflows/microsite.yml>`_.
The instructions below is in case the workflow is failing, or manual deployment is needed.


.. admonition:: FYI
    :class: tip

    The microsite is deployed to the ``gh-pages`` branch, created as an `orphaned branch <https://git-scm.com/docs/git-checkout#Documentation/git-checkout.txt---orphanltnew-branchgt>`_ from ``master``.
    The contents of the ``microsite/`` directory on ``master`` are mirrored in the root directory in ``gh-pages``.

Manual Deployment
~~~~~~~~~~~~~~~~~

After changes to ``microsite/`` have been merged into ``master``:

.. code-block:: sh

    $ git fetch
    $ git checkout gh-pages
    $ git checkout master microsite

    # move all contents of microsite/ to root
    $ mv microsite/css/* css/
    $ mv microsite/fonts/* fonts/
    $ mv microsite/images/* images/
    $ mv microsite/js/* js/
    $ mv microsite/index.html .

    # remove microsite
    $ rm -rf microsite

    # commit & push
    $ git commit -am {{ commit message }}
    $ git push origin gh-pages

It may take a few minutes for GitHub pages to update.


Creating a Package Release
--------------------------

Instructions on how to release a new version of a Klio package can be found `here <https://github.com/spotify/klio/blob/master/RELEASING.rst>`_.

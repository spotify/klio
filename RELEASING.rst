Releasing a Klio Package
========================

Klio packages are manually released and uploaded to PyPI (versus via CI/CD).
The following instructions assume you're an `approved maintainer <https://github.com/spotify/klio/blob/master/CODEOWNERS>`_ of Klio.


*Credit: These instructions are heavily inspired by* |this write up|_.

.. it's impossible to nest formatting - aka have a link inside an italicized line. So this is a hack from https://stackoverflow.com/questions/4743845/format-text-in-a-link-in-restructuredtext

.. |this write up| replace:: *this write up*
.. _this write up:  https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty/

.. _initial-release-setup:

Initial Setup
-------------

**Note:** This section will only need to be completed once, during your first time releasing a Klio package.
You may skip to `Releasing <#releasing>`_ for subsequent releases.

Create a Releasing Virtualenv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Since it's project-agnostic, we can make a general "releasing" virtualenv with the packages we need:

.. code-block:: sh

    $ pyenv virtualenv klio-release
    $ pyenv activate klio-release
    (klio-release) $ pip install -U pip setuptools wheel twine restview

Tools used:

* ``pip`` used to install packages (duh, but this process includes sanity checks)
* `setuptools <https://pypi.org/project/setuptools/>`_ and `wheel <https://pypi.org/project/wheel/>`_ is used to build packages (both source & wheel distributions)
* `twine <https://pypi.org/project/twine/>`_ to upload packages securely to PyPI
* `restview <https://pypi.org/project/restview/>`_ to generate a preview of a package's `long_description <https://packaging.python.org/guides/making-a-pypi-friendly-readme/>`_ (it's easy to mess up) which is what populate's the package's landing page on PyPI

Initial Setup for Releasing
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a PyPI account
^^^^^^^^^^^^^^^^^^^^^

There are two public PyPI servers we will upload to: **testing** and **production**.
For the production server, we need will add more security precautions to our account:

1. Turn on multi-factor authentication in your `account's settings <https://pypi.org/manage/account/>`_.
2. Generate an API token (also in your `account's settings <https://pypi.org/manage/account/>`_).
    1. Token name can be anything.
    2. Scope should be "Entire account (all projects)" since we will have multiple Klio packages.
    3. Save the token for the next step below.


**Note:** It's recommended to also follow the above two steps for `your account <https://test.pypi.org/manage/account/>`_ on the test PyPI server.

The testing index server is for testing uploads, and making sure they are installable/usable before releasing to production PyPI.

The production index server is for *...you guessed it...* releasing to production.


Create/Update ``~/.pypirc`` file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you do not have a ``~/.pypirc`` file yet, create one.

Then, update it with the following information:

.. code-block:: ini

    [distutils]
    index-servers=
        prod
        test

    [prod]
    repository = https://upload.pypi.org/legacy/
    username = __token__
    password = <YOUR PYPI TOKEN>

    [test]
    repository = https://test.pypi.org/legacy/
    username = <YOUR USERNAME>
    password = <YOUR PASSWORD>

**Note:** If you also created a token for your account on the test PyPI server, your ``~/.pypirc`` file should look like the following:

.. code-block:: ini

    [distutils]
    index-servers=
        prod
        test

    [prod]
    repository = https://upload.pypi.org/legacy/
    username = __token__
    password = <YOUR PROD PYPI TOKEN>

    [test]
    username = __token__
    password = <YOUR TEST PYPI TOKEN>

(Recommended) GPG Key Setup
^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is to ensure git commits and package releases are really from us.
This also gives you the "verified" tag next to your name in GitHub.

1. If you do not one already, generate a GPG key (`macOS <https://gpgtools.org/>`_, `everyone else + those choosing not to use GPGtools for Mac <https://www.gnupg.org/gph/en/manual/c14.html>`_).
    1. If you're unfamiliar with GPG/PGP, read up `here <https://digitalguardian.com/blog/what-pgp-encryption-defining-and-outlining-uses-pgp-encryption>`_ (GPG is an implementation of PGP); here's a decent `Quora post <https://www.quora.com/What-is-a-GPG-key-and-how-do-I-create-it>`_ as well.
    2. (Re-)familiarize yourself with `best practices <https://riseup.net/en/security/message-security/openpgp/gpg-best-practices>`_ for PGP/GPG keys.
2. Add your GPG key to GitHub by following `these docs <https://docs.github.com/en/github/authenticating-to-github/adding-a-new-gpg-key-to-your-github-account>`_.
3. Configure git to use your GPG key by following `these instructions <https://docs.github.com/en/github/authenticating-to-github/telling-git-about-your-signing-key#telling-git-about-your-gpg-key>`_.
4. Automatically sign git commits and tags:

.. code-block:: sh

    git config --global commit.gpgsign true
    git config --global tag.gpgsign true

5. Going forward, when releasing (also mentioned below), sign newly built packages before releasing with the ``--sign`` flag, e.g. ``twine upload r pypi -sign /path/to/package_name/dist/package-name-1.2.3*``.


Releasing
---------

Prepare Release
~~~~~~~~~~~~~~~

Before building and uploading, we need to make the required release commit(s) for a pull request.

Update Changelog
^^^^^^^^^^^^^^^^

**FYI:** Each package has its changelog in ``docs/src/reference/<package>/changelog.rst``. The details of the latest release is seen on the package's PyPI project page (`for example <https://pypi.org/project/klio/>`_, see "Release Information" at the bottom).
This is generated automatically in the package's ``setup.py::get_long_description`` function.

Steps to Update
***************

1. Navigate to the package's changelog in ``docs/src/reference/<package>/changelog.rst``.
2. There should be a header for the current version plus ``(UNRELEASED)``, e.g. ``1.2.3 (UNRELEASED)``. Update ``(UNRELEASED)`` to the package's release date, e.g. ``1.2.3 (2021-01-01)``.
3. Make sure the changelog entries for the version are up to date. Ideally, it's all populated as contributors have added an item to the changelog along with their pull requests. If not, refer to the commits between this release and the last release for the package and populate the changelog accordingly. See `below <#changelog-format>`_ for how the changelog should be formatted. See the `Changelog section in CONTRIBUTING.rst <https://github.com/spotify/klio/blob/master/CONTRIBUTING.rst#changelog>`_ when writing an entry for a pull request when not releasing.
4. Commit changes with the message "Prepare release <package> v1.2.3".

Changelog Format
****************

1. Each changelog entry should describe the change with the users as the audience. For example, "Dropped support for Python 3.5", or "Added support for configuration templating." If the change doesn't affect the user, then it should probably not have an entry.

2. Each changelog entry should fall under one of the following sections:

* Added
* Fixed
* Removed
* Changed
* Dependencies Updated

3. The ``changelog.rst`` file should follow the following template (note that not all sections need to have content):

.. code-block:: rst

    Changelog
    =========

    1.2.4 (UNRELEASED)
    ------------------

    Changed
    *******

    * ``antigravity`` submodule is now located under ``flying_pigs`` module with a redirect from the original ``frozen_hell.antigravity`` location.

    Dependencies Updated
    ********************

    * Minimum version of ``teleportation`` dependency now at ``9.0.0`` due to API changes.

    1.2.3 (2021-01-01)
    ------------------

    Added
    *****

    * Added support for an invisibility cloak (See `KEP 999 <https://docs.klio.io/en/latest/keps/kep-999.html>`_).

    Fixed
    *****

    * Turning on flying mode now correctly detects elevation (See `PR 99999 <https://github.com/spotify/klio/pull/99999>`_).


    1.2.2 (2020-12-01)
    ------------------

    Removed
    *******

    * Deprecated support for Python 2.9
    * Removed unused ``nightvision`` dependency.


Update Version
^^^^^^^^^^^^^^

**Attention:** This step should be done with the **virtualenv of the package** activated (**not** the virtualenv needed for releasing).
This virtualenv should have the ``dev`` extras package installed, i.e. ``pip install -e ".[dev]"`` which includes the ``bumpversion`` library.

.. code-block:: sh

    # within the dir of the package you're releasing
    $ pyenv activate $KLIO_PACKAGE_VIRTUALENV

    # make sure the git tree is clean
    ($KLIO_PACKAGE_VIRTUALENV) $ git status
    On branch master
    nothing to commit, working tree clean

    # create a release branch
    ($KLIO_PACKAGE_VIRTUALENV) $ git checkout -b $RELEASE_VERSION

    # run bumpversion for the release type (major, minor, patch, etc)
    ($KLIO_PACKAGE_VIRTUALENV) $ bumpversion $RELEASE_TYPE

    # push to a branch for origin remote (unless you named origin differently)
    # WITH TAGS!!;  it’s helpful to others to prefix your branch with your username
    ($KLIO_PACKAGE_VIRTUALENV) $ git push --tags origin HEAD:$USER/$RELEASE_VERSION

Then create a pull request for review.

Once approved and merged:

.. code-block:: sh

    # within the root of the klio repo
    $ git checkout master
    $ git pull --rebase origin master

    # optional: delete local release branch
    $ git branch -d $RELEASE_VERSION


Build Artifact
~~~~~~~~~~~~~~

Using your ``klio-release`` virtualenv from `above <#create-a-releasing-virtualenv>`_:

.. code-block:: sh

    # checkout the git tag made for the release
    $ git checkout $TAG_NAME

    # within the dir of the package you're releasing
    $ pyenv activate klio-release

    # clear out previous builds to avoid confusion and mistaken uploads
    (klio-release) $ rm -rf build dist

    # build both wheel and source dists
    (klio-release) $ python setup.py build sdist bdist_wheel


You should now have two items in the dist directory. For example:

.. code-block:: sh

    dist
    ├── $KLIO_PACKAGE-1.2.3-py2.py3-non-any.whl
    └── $KLIO_PACKAGE-1.2.3.tar.gz


Sanity Check: Test Long Description
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For Klio packages, the `long description <https://packaging.python.org/guides/making-a-pypi-friendly-readme/>`_ gets generated within each project's ``setup.py`` by combining its ``README.rst``, the latest entry of its changelog, and the ``README.rst`` in the root of the repository.
Let's make sure it gets generated correctly.

.. code-block:: sh

    # use twine to surface any parsing issues of restructured text
    (klio-release) $ twine check dist/*

    # view the contents of the package’s long description
    (klio-release) $ restview --long-description

The ``restview`` command will start a local server and launch a new tab in your browser previewing the long description (which should then match what is rendered in the project's PyPI page at ``https://pypi.org/project/$KLIO_PACKAGE``, minus styling/CSS and not-yet-published description updates).

Make any necessary edits for ``twine check`` to pass, and for the long description to be parsed & rendered correctly via ``restview``.


Sanity Check: Test Installation Locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create two virtualenvs to test installation.
The virtualenvs are needed to test both the source (``.tar.gz``) and the wheel (``.whl``) distributions.

For **extra** sanity checks, create a virtualenv per Python version supported for both source and wheel testing (e.g. ``36-sdist``, ``36-whl``, ``37-sdist``, ``37-whl``, and so on).

**Example workflow with python 3.6 testing the source distribution:**

.. code-block:: sh

    # deactivate the releasing-specific virtualenv
    (klio-release) $ deactivate  # or source deactivate

    # create virtualenv your standard way, with $PY36_VERSION referring to the
    # full version of Python available,  e.g. 3.6.11:
    $ pyenv virtualenv $PY36_VERSION 36-sdist
    $ pyenv activate 36-sdist
    (36-sdist) $

    # be sure to be in a *different* directory than the repo to avoid
    # misleading successful installs & imports
    (36-sdist) $ cd ~

    # install the just-built relevant distribution
    (36-sdist) $ pip install path/to/$KLIO_PKG_DIR/dist/$KLIO_PACKAGE-1.2.3.tar.gz

    # test the package is correctly installed
    (36-sdist) $ python -c 'import $KLIO_PACKAGE; print($KLIO_PACKAGE.__version__)'
    '1.2.3'

If successful, you can deactivate and delete the virtualenv:

.. code-block:: sh

    (36-sdist) $ deactivate  # or source deactivate
    $ pyenv virtualenv delete 36-sdist

**Repeat** for the remaining test virtualenvs.


Upload to Testing Server
~~~~~~~~~~~~~~~~~~~~~~~~

Next, we will use ``twine`` to securely upload the new release to the PyPI testing server:

.. code-block:: sh

    # within the dir of the package you're releasing
    $ pyenv activate klio-release

    # upload both the source & wheel distributions in one command
    (klio-release) $ twine upload -r test dist/$KLIO_PACKAGE*

**Attention:** The ``-r`` in the ``twine upload ...`` command refers to the name of the server defined in your ``~/.pypirc``.


Sanity Check: Project Page Description
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Make sure the package's `long description <#sanity-check- test-long-description>`_ looks okay on its test PyPI project page at ``https://test.pypi.org/project/$KLIO_PACKAGE``.

If something is messed up, make the necessary edits.
To test again, unfortunately **you can not upload a package with the same version**.
You can, however, temporarily add a `supported suffix <https://www.python.org/dev/peps/pep-0440/>`_ to the version (e.g. ``1.2.3.dev1``) to re-upload to the test server to try again.
Just be sure to **remove the suffix** when moving on.

.. _test-testing-install:


Sanity Check: Test Installation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Test the package installation again (`just like above <#sanity-check-test-installation-locally>`_) by installing it via ``pip``:

.. code-block:: sh

    # deactivate `klio-release` virtualenv
    (klio-release) $ deactivate  # or source deactivate

    # and create a temp testing virtualenv - do NOT reuse the one from earler
    $ pyenv virtualenv test-install
    $ pyenv activate test-install

    # install explicitly from staging PyPI
    (test-install) $ pip install $KLIO_PACKAGE -i https://testpypi.python.org/pypi
    (test-install) $ python -c 'import $KLIO_PACKAGE; print($KLIO_PACKAGE.__version__)'

    # deactivate & delete test env
    (test-install) $ deactivate  # or source deactivate
    $ pyenv virtualenv-delete test-install


Upload to Production Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, we will use ``twine`` again to securely upload the new release to the PyPI **production** server:

.. code-block:: sh

    # within the dir of the package you're releasing
    $ pyenv activate klio-release

    # upload both the source & wheel distributions in one command
    (klio-release) $ twine upload -r prod dist/$KLIO_PACKAGE*

**Attention:** The ``-r`` in the ``twine upload ...`` command refers to the name of the server defined in your ``~/.pypirc``.


Sanity Check: Project Page Description (again)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Like `above <#sanity-check-project-page-description>`_, make sure the package's `long description <#sanity-check- test-long-description>`_ looks okay on its PyPI project page at ``https://pypi.org/project/$KLIO_PACKAGE``.
If there are any issues, decide whether or not it's worth it to create a post release (as it’s not possible to upload a package with the same version). Most likely, it can wait a release cycle.


Sanity Check: Test Installation (again)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Test the package installation again (`just like above <#sanity-check-test-installation>`_) by installing it via ``pip``:

.. code-block:: sh

    # deactivate `klio-release` virtualenv
    (klio-release) $ deactivate  # or source deactivate

    # and create a temp testing virtualenv - do NOT reuse the one from earler
    $ pyenv virtualenv test-install
    $ pyenv activate test-install

    # install explicitly from public PyPI
    (test-install) $ pip install $KLIO_PACKAGE -i https://pypi.org/simple
    (test-install) $ python -c 'import $KLIO_PACKAGE; print($KLIO_PACKAGE.__version__)'

    # deactivate & delete test env
    (test-install) $ deactivate  # or source deactivate
    $ pyenv virtualenv-delete test-install

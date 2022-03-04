Releasing a Klio Package
========================

Klio packages releases are mostly automated via GitHub Actions, but can also be released manually if needed.

Note on Version Scheme
----------------------

All Klio packages are released synchronously and use `calendar versioning <https://docs.klio.io/en/stable/release_notes/21.2.0.html#new-versioning-scheme-synchronization>`_,
following the scheme of ``YY.MM.MICRO(suffixN)``:

* no zero-padding for the month (e.g. ``21.2.0`` for February release);
* ``.MICRO`` starts at ``0`` , so ``YY.MM.0`` is the first release for the month;
* ``YY.MM.0`` builds signify the scheduled release for the month
* every ``MICRO`` increment after ``0`` is an unscheduled release (a hotfix, patch of some sort, etc)
* The ``suffixN`` is optional, and is used to signify pre-releases (e.g. ``21.2.0.dev1`` for a development pre-release of ``21.2.0``, or ``21.2.0rc1`` for a release candidate of ``21.2.0``)

Read more about package versioning in `PEP-440 <https://www.python.org/dev/peps/pep-0440>`_.


Release Process
---------------

While the release process is mostly automated via GitHub Actions, there is still some manual work to prepare the release.

Terminology
~~~~~~~~~~~

general release
    A `final release <https://www.python.org/dev/peps/pep-0440/#final-releases>`_ where the version does not have any suffix.
    For example, ``21.2.0``.

pre-release
    A `preview release <https://www.python.org/dev/peps/pep-0440/#pre-releases>`_ made available before the general release.
    For example, ``21.2.0rc1``, ``21.2.0beta1``, ``21.2.0.dev1``.

post-release
    A `release after a general release <https://www.python.org/dev/peps/pep-0440/#post-releases>`_ that adds very minor fixes to a general release.
    For example, ``21.2.0.post2``.

devevelopmental release
    A `type of pre-release <https://www.python.org/dev/peps/pep-0440/#developmental-releases>`_ typically meant for maintainers' development cycle and not meant for Klio users.
    Also known as a "dev release". For example, ``21.2.0.dev1``.


Checklist Overview
~~~~~~~~~~~~~~~~~~

1. `Prepare a remote branch for the general release <#prepare-remote-general-release-branch>`_
2. `Prepare local/individual release branch based off of remote general branch <#a-create-individual-release-branch>`_
3. `Update changelogs for each package <#b-update-packages-changelog>`_
4. `Bump versions for each package <#c-update-version>`_
5. `Update general release notes (for non-pre-releases) <#d-update-release-notes>`_
6. `Create a genereal release tag <#e-create-general-release-tag>`_
7. `Push with tags to a new remote branch <#f-push-changes-to-origin>`_
8. `Make a PR to the remote general release branch (from Step 1) from your individual release branch <#a-pr-to-release-branch>`_
9. `Make a PR from general release branch to develop <#b-pr-to-develop-branch>`_
10. `Make a PR from develop branch to master <#c-pr-to-master-branch>`_
11. `Execute GitHub workflows that build and upload the Python packages <#execute-github-workflows>`_


1. Prepare Remote General Release Branch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Skip this step for developmental releases and continue to the next step.**

We first need to create a release branch based off of ``develop``.

Locally, run the following commands, replacing the ``<VERSION>`` with the target general release version in the format of ``YY.MM.MICRO`` (no ``suffixN``):

.. code-block:: sh

    $ git checkout develop
    $ git pull --rebase origin develop

    # if the release branch is NOT yet created
    # or no new code needs to be added to the release
    $ git checkout -b release-<VERSION>
    $ git push origin release-<VERSION>

    # if the release branch already exists
    # and additional code needs to be added to the release
    $ git checkout release-<VERSION>
    $ git merge develop

The changes made in the following `Prepare Release <#2-prepare-release>`_ step will be in a pull request made against this ``release-<VERSION>`` branch.


2. Prepare Release
~~~~~~~~~~~~~~~~~~

2a. Create Individual Release Branch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Non-developmental Releases
**************************

From the ``release-<VERSION>``, create a new local branch.

For instance, if a pre-release is being made, then name the new branch with the ``suffixN`` added.
If we're making a pre-release of ``21.2.0``,  e.g.:

.. code-block:: sh

    $ git checkout release-21.2.0  # make sure we start from the correct branch
    $ git checkout -b <USER>/release-21.2.0

Make the changes in the next steps on this branch. This branch will eventually be merged into the ``release-<VERSION>`` branch via pull request.

Developmental Releases
**********************

From the ``develop``, create a new local branch.

For instance, if a pre-release is being made, then name the new branch with the ``suffixN`` added.
If we're making a pre-release of ``21.2.0``,  e.g.:

.. code-block:: sh

    $ git checkout develop
    $ git pull --rebase origin develop
    $ git checkout -b <USER>/release-21.2.0.dev1

Make the changes in the next steps on this branch. This branch will eventually be merged into the ``develop`` branch via pull request.

2b. Update Packages' Changelog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For each Klio package,

1. Navigate to the package's changelog in ``docs/src/reference/<package>/changelog.rst``.
2. If making a pre-release: If it doesn't exist already, add the header for the current version plus ``(UNRELEASED)``, e.g. ``21.2.0 (UNRELEASED)`` (see `below <#changelog-format>`_ for an example).
3. If making a general release: Update the header from ``(UNRELEASED)`` to the package's release date, e.g. ``21.2.0 (2021-01-01)`` (see `below <#changelog-format>`_ for an example).
4. If there are no changes for a particular package, add the message ``No changes - bump version to sync with <version> release.`` (`an example <https://docs.klio.io/en/stable/reference/core/changelog.html#core-21-9-0>`_)
5. Add the necessary rST references and labels (see `below <#changelog-format>`_ for what this means). These help us avoid duplicate text for when we prepare the `Release Notes <#d-update-release-notes>`_ and the packages' long description.
6. Make sure the changelog entries for the version are up to date. Ideally, it's all populated as contributors have added an item to the changelog along with their pull requests. If not, refer to the commits between this release and the last release for the package and populate the changelog accordingly. See `below <#changelog-format>`_ for how the changelog should be formatted.
7. Commit changes (on your local branch from the ``release-<VERSION>`` branch from `above <#a-create-individual-release-branch>`_) with the message ``[<pkg>] Prepare changelog for <version>``. There should be one commit per package.


2c. Update Version
^^^^^^^^^^^^^^^^^^

Option 1: Bumpversion
*********************

**Attention:** ``bumpversion`` is unsupported when making pre-releases.
Follow the `manual steps <#option-2-manually>`_ when updating the version to a pre-release.

**Attention:** This step should be done with the **virtualenv of the package** activated (**not** the virtualenv needed for releasing).
This virtualenv should have the ``dev`` extras package installed, i.e. ``pip install -e ".[dev]"`` which includes the ``bumpversion`` library.

For each package:

.. code-block:: sh

    # within the dir of the package you're releasing
    $ pyenv activate $KLIO_PACKAGE_VIRTUALENV

    # make sure you're on the local release branch
    ($KLIO_PACKAGE_VIRTUALENV) $ git checkout $LOCAL_RELEASE_BRANCH

    # make sure the git tree is clean
    ($KLIO_PACKAGE_VIRTUALENV) $ git status
    On branch master
    nothing to commit, working tree clean

    # run bumpversion for the release type (major, minor, patch, etc)
    ($KLIO_PACKAGE_VIRTUALENV) $ bumpversion $RELEASE_TYPE


Option 2: Manually
******************

For each package:

1. Update the version in ``<pkg>/setup.cfg``.
2. Update the version in ``<pkg>/src/__init__.py``.
3. Commit and tag:

.. code-block:: sh

    $ git commit -m "[<dir>] Bump version: <old version> → <new version>"
    $ git tag <dir>-<version> -m "[<dir>] Bump version: <old version> → <new version>"

For example, for ``klio-core`` when bumping from ``21.9.0rc1`` to ``21.9.0``:


.. code-block:: sh

    $ git commit -m "[core] Bump version: 21.9.0rc1 → 21.9.0"
    $ git tag core-21.9.0 -m "[core] Bump version: 21.9.0rc1 → 21.9.0"


2d. Update Release Notes
^^^^^^^^^^^^^^^^^^^^^^^^

**For general releases only. Skip for dev, pre- and post-releases.**

1. In ``docs/src/release_notes/``, make a copy of ``template.rst`` for the version being released, e.g. ``21.2.0.rst``.
    1. Populate the template with major user-facing changes, announcements, etc. Refer to previous release notes for ideas.
    2. Be sure to include the ``Changes`` section that links to individual changelogs (see the `aside below <#aside-where-are-these-references-used>`_ for how changelogs get pulled in).
2. In ``docs/src/release_notes/index.rst``, add the file name (without the ``.rst`` extension) under the ``.. toctree::``. Be sure to add the file name at the top (reverse-chronological order). Making this change updates the contents of the `Release Notes landing page <https://docs.klio.io/en/stable/release_notes/index.html>`_ and the left sidebar.
3. Commit changes (on your branch from the ``release-<VERSION>`` branch from `above <#a-create-individual-release-branch>`_) with the message ``Prepare <version> release notes``.

2e. Create General Release Tag
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
**For general and post-releases only. Skip for dev- and pre-releases.**

Create a general release ``git`` tag in the form of ``YY.MM.build`` (no ``release-`` or ``<pkg>-`` prefixes).
This tells Read The Docs (where `docs.klio.io <https://docs.klio.io>`_ is hosted) to create a new version of the documentation based off of this commit.
This allows users to be able to navigate to the documentation for their specific Klio version.

.. code-block:: sh

    $ git tag <YY.MM.build>
    # or if a post-release
    $ git tag <YY.MM.buildpostN>

2f. Push Changes to Origin
^^^^^^^^^^^^^^^^^^^^^^^^^^

Push these changes to a remote branch **with tags**, replacing ``<USER>`` with your name and version information (including any suffixes/pre-releases):

.. code-block:: sh

    $ git push --tags origin HEAD:<USER>/release-<YY.MM.build(suffixN)>

3. Make Pull Requests Into Respective Branches
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Developmental Releases
^^^^^^^^^^^^^^^^^^^^^^

3a. PR to ``develop`` branch
******************************

Within GitHub, create a pull request into the ``develop`` branch from your branch that you pushed to in the `step above <#f-push-changes-to-origin>`_.
The title of the pull request can just be ``Release <YY.MM.build(devN)>`` with no body.

Once reviewed, go ahead and merge.

If there are any changes that need to be made before merging, be mindful to re-tag as needed.
To see if the tags are attached to the correct commit, you can run this command:

.. code-block:: sh

    $ git log --pretty=format:"%C(yellow)%h%Cred%d\\ %Creset%s%Cblue\\ [%cn]" --decorate
    # Example output
    626f48e (tag: 21.9.0) Release notes for 21.9.0 [Lynn Root]
    f2ba7fb (tag: exec-21.9.0) [exec] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    a2ec9de (tag: core-21.9.0) [core] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    5e6061e (tag: lib-21.9.0) [lib] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    bfb3b71 (tag: cli-21.9.0) [cli] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]

If one or more tags have been lost due to edits, grab the commit where the tag should be, and run:

.. code-block:: sh

    $ git tag -f <pkg>-<YY.MM.build(suffixN)> <commit>
    # for example
    $ git tag -f exec-21.9.0 f2ba7fb

    # after all is correctly tagged, push using --force-with-lease and --tags
    $ git push --tags --force-with-lease origin HEAD:<USER>/release-<YY.MM.build(suffixN)>


You can delete your branch once done.
Continue on to `executing the GitHub Workflows <#4-execute-github-workflows>`_.


Non-Developmental Releases
^^^^^^^^^^^^^^^^^^^^^^^^^^

3a. PR to ``release-*`` branch
******************************

**For non-developmental releases only. If making a dev release, go to the next step.**

Within GitHub, create a pull request into the ``release-*`` branch from your branch that you pushed to in the `step above <#f-push-changes-to-origin>`_.
The title of the pull request can just be ``Release <YY.MM.build(suffixN)>`` with no body.

Once reviewed, go ahead and merge.

If there are any changes that need to be made before merging, be mindful to re-tag as needed.
To see if the tags are attached to the correct commit, you can run this command:

.. code-block:: sh

    $ git log --pretty=format:"%C(yellow)%h%Cred%d\\ %Creset%s%Cblue\\ [%cn]" --decorate
    # Example output
    626f48e (tag: 21.9.0) Release notes for 21.9.0 [Lynn Root]
    f2ba7fb (tag: exec-21.9.0) [exec] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    a2ec9de (tag: core-21.9.0) [core] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    5e6061e (tag: lib-21.9.0) [lib] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]
    bfb3b71 (tag: cli-21.9.0) [cli] Bump version: 21.9.0rc1 → 21.9.0 [Lynn Root]

If one or more tags have been lost due to edits, grab the commit where the tag should be, and run:

.. code-block:: sh

    $ git tag -f <pkg>-<YY.MM.build(suffixN)> <commit>
    # for example
    $ git tag -f exec-21.9.0 f2ba7fb

    # after all is correctly tagged, push using --force-with-lease and --tags
    $ git push --tags --force-with-lease origin HEAD:<USER>/release-<YY.MM.build(suffixN)>


You can delete your branch once done.

Here's an example `pull request to a release-* branch <https://github.com/spotify/klio/pull/227>`_ for reference.

3b. PR to ``develop`` branch
****************************

Once the above pull request is merged, make a pull request from the ``release-*`` branch to ``develop``.
The title of the pull request can be the same as above.

A review is not necessary. If comfortable, merge without review.

**Do not** delete the ``release-*`` branch once it's merged into ``develop``.

Here's an example `pull request to the develop branch <https://github.com/spotify/klio/pull/228>`_ for reference.

3c. PR to ``master`` branch
***************************

Once the above pull request is merged, make a pull request from the ``develop`` branch to ``master``.
The title of the pull request can be the same as above.

A review is not necessary. If comfortable, merge without review.

**Do not** delete the ``develop`` branch once it's merged into ``master``.

Here's an example `pull request to the master branch <https://github.com/spotify/klio/pull/229>`_ for reference.

4. Execute GitHub Workflows
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once everything is merged above, we're ready to release the packages!

1. Navigate to the `releasing GitHub workflow <https://github.com/spotify/klio/actions/workflows/release.yml>`_ (titled "Build & Release to PyPI").
2. In the upper-ish right corner, navigate to the drop down called "Run workflow".
3. Click on "Run workflow" and select the specific ``release-*`` branch. If this is a developmental release, then select the ``develop`` branch.
4. Once the release branch is selected, hit the green button for "Run workflow".

This will kick off the following steps:

* Build the wheel and source distribution of the particular package;
* Check both artifacts' long description via ``twine``;
* Locally test installation of both artifacts in a virtualenv and make sure the package is import-able;
* Upload the artifacts of the package to the Test PyPI server;
* Test installation of the package from the Test PyPI server;
* Uplaod the artifacts of the package to the Prod PyPI server;
* Test installation of the package from the Prod PyPI server.

It will first do the above steps for ``klio-core`` (as all other Klio packages depend on ``klio-core``) .
Once done, then ``klio``, ``klio-cli``, and ``klio-audio`` is done simultaneously.
After that, ``klio-exec`` and ``klio-devtools`` (which depend on ``klio`` and ``klio-cli`` respectively) is built & uploaded.

If any step fails, then the whole workflow fails.
Refer to the workflow's logs to debug any failures.

Manual Releasing
----------------

**NOTICE:** Follow these instructions when making a release via GitHub Actions is **not** possible.

The following instructions assume:

* you're an `approved maintainer <https://github.com/spotify/klio/blob/master/CODEOWNERS>`_ of Klio;
* you've completed the `initial setup instructions below <#initial-release-setup>`_.


Prepare Release
~~~~~~~~~~~~~~~

Before building and uploading, we need to make the required release commit(s) for a pull request.

Follow the steps above for:

1. `Prepare Remote General Release Branch <#prepare-remote-general-release-branch>`_
2. `Prepare the Release <#prepare-release-shared>`_
3. `Create Respective Pull Requests <#make-pull-requests-into-respective-branches>`_


Build Artifact
~~~~~~~~~~~~~~

Using your ``klio-release`` virtualenv from the `one-time setup below <#create-a-releasing-virtualenv>`_:

.. code-block:: sh

    # checkout the git tag made for the release
    $ git checkout $TAG_NAME

    # within the dir of the package you're releasing
    $ pyenv activate klio-release

    # clear out previous builds to avoid confusion and mistaken uploads
    (klio-release) $ rm -rf build dist

    # build both wheel and source dists
    (klio-release) $ python -m build --sdist --wheel --outdir dist/ .


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

For **extra** sanity checks, create a virtualenv per Python version supported for both source and wheel testing (e.g. ``37-sdist``, ``37-whl``, ``38-sdist``, ``38-whl``, and so on).

**Example workflow with python 3.7 testing the source distribution:**

.. code-block:: sh

    # deactivate the releasing-specific virtualenv
    (klio-release) $ deactivate  # or source deactivate

    # create virtualenv your standard way, with $PY37_VERSION referring to the
    # full version of Python available,  e.g. 3.7.7:
    $ pyenv virtualenv $PY37_VERSION 37-sdist
    $ pyenv activate 37-sdist
    (37-sdist) $

    # be sure to be in a *different* directory than the repo to avoid
    # misleading successful installs & imports
    (37-sdist) $ cd ~

    # install the just-built relevant distribution
    (37-sdist) $ pip install path/to/$KLIO_PKG_DIR/dist/$KLIO_PACKAGE-1.2.3.tar.gz

    # test the package is correctly installed
    (37-sdist) $ python -c 'import $KLIO_PACKAGE; print($KLIO_PACKAGE.__version__)'
    '1.2.3'

If successful, you can deactivate and delete the virtualenv:

.. code-block:: sh

    (37-sdist) $ deactivate  # or source deactivate
    $ pyenv virtualenv delete 37-sdist

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

Make sure the `package's long description <#sanity-check-test-long-description>`_ looks okay on its test PyPI project page at ``https://test.pypi.org/project/$KLIO_PACKAGE``.

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

Like earlier, make sure the package's long description looks okay on its PyPI project page at ``https://pypi.org/project/$KLIO_PACKAGE``.

If there are any issues, decide whether or not it's worth it to create a post release (as it’s not possible to upload a package with the same version). Most likely, it can wait a release cycle.


Sanity Check: Test Installation (again)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Test the package installation again by installing it via ``pip``:

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


Appendix
--------

.. _initial-release-setup:

Initial Setup
~~~~~~~~~~~~~

**Note:** This section will only need to be completed once, during your first time manually releasing a Klio package.
You may skip to `preparing the manual release <#prepare-release>`_ for subsequent manual releases.

Create a Releasing Virtualenv
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Since it's project-agnostic, we can make a general "releasing" virtualenv with the packages we need:

.. code-block:: sh

    $ pyenv virtualenv klio-release
    $ pyenv activate klio-release
    (klio-release) $ pip install -U pip setuptools build twine restview

Tools used:

* ``pip`` used to install packages (duh, but this process includes sanity checks)
* `setuptools <https://pypi.org/project/setuptools/>`_ and `build <https://pypi.org/project/build/>`_ is used to build packages (both source & wheel distributions)
* `twine <https://pypi.org/project/twine/>`_ to upload packages securely to PyPI
* `restview <https://pypi.org/project/restview/>`_ to generate a preview of a package's `long_description <https://packaging.python.org/guides/making-a-pypi-friendly-readme/>`_ (it's easy to mess up) which is what populate's the package's landing page on PyPI

Initial Setup for Releasing
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a PyPI account
*********************

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
********************************

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
***************************

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


Changelog Format
~~~~~~~~~~~~~~~~

**Note:** Below is the format for writing ``docs/src/reference/<package>/changelog.rst`` files.
When writing an entry for a changelog as you're making a particular change, see the `Changelog section in CONTRIBUTING.rst <https://github.com/spotify/klio/blob/master/CONTRIBUTING.rst#changelog>`_.


1. Header
^^^^^^^^^

A release's header may already exist if changelog entries were added as new changes were committed.

If not, at the top of the ``changelog.rst`` file, under the ``Changelog`` title, each release should have a header section of the general release version and the date it was released.


.. code-block:: rst

    Changelog
    =========

    21.2.0 (2021-02-13)
    -------------------


If it's a pre-release, do **not** include the pre-release suffix, and leave it as ``(UNRELEASED)``:

.. code-block:: rst

    Changelog
    =========

    21.3.0 (UNRELEASED)
    -------------------


Releases are listed in reverse chronological order (latest at the top).


2. Changelog Entries
^^^^^^^^^^^^^^^^^^^^

Hopefully, these entries have already been added when the changes were originally made.

Each changelog entry should describe the change with the users as the audience.
For example, "Dropped support for Python 3.5", or "Added support for configuration templating."
If the change doesn't affect the user, then it should probably not have an entry.


Each changelog entry should fall under one of the following sections:

* Added
* Fixed
* Removed
* Changed
* Dependencies Updated

Not all sections need to have content.

If there are *no* changes, then just write ``No changes - bump version to sync with <version> release.`` (no section needed).

Example:

.. code-block:: rst

    21.3.0 (UNRELEASED)
    -------------------

    Changed
    *******

    * ``antigravity`` submodule is now located under ``flying_pigs`` module with a redirect from the original ``frozen_hell.antigravity`` location.

    Added
    *****

    * Added support for an invisibility cloak (See `KEP 999 <https://docs.klio.io/en/latest/keps/kep-999.html>`_).


    21.2.0 (2021-02-13)
    -------------------

    No changes - bump version to sync with 21.2.0 release.


    21.1.0 (2021-01-01)
    -------------------

    Fixed
    *****

    * Turning on flying mode now correctly detects elevation (See `PR 99999 <https://github.com/spotify/klio/pull/99999>`_).

    Removed
    *******

    * Deprecated support for Python 2.9
    * Removed unused ``nightvision`` dependency.


3. rST References
^^^^^^^^^^^^^^^^^

Each release section needs a couple of rST references.

**Why?** This enables other areas of our documentation to refer to or pull in content so we don't have to repeat ourselves.
See the `aside <#aside-where-are-these-references-used>`_ below for more information.

1. Reference to Header
**********************

Before each `release header <#header>`_, a rST reference is needed, using the format ``.. _<pkg>-YY.MM.build:``, followed by an empty newline.
For example, for the ``klio-core`` package:

.. code-block:: rst

    Changelog
    =========

    .. _core-21.3.0:

    21.3.0 (UNRELEASED)
    -------------------


2. Start & End Reference Labels
*******************************

Two tags are needed to surround the content of the changelog for each release.
The first one marks the start of the content, is in the format of ``.. start-YY.MM.build``, and is placed after the `release header <#header>`_.
The second one marks the end of the content, is in the format of ``.. end-YY.MM.build``, and is placed after the last changelog entry of the release (before the `reference to the header <#reference-to-header>`_ of the previous release).

Example:

.. code-block:: rst

    Changelog
    =========

    .. _core-21.3.0:

    21.3.0 (UNRELEASED)
    -------------------

    .. start-21.3.0

    <Entries>

    .. end-21.3.0


    .. _core-21.2.0:

    21.2.0 (2021-02-13)
    -------------------


Aside: Where are these references used?
***************************************

Both of these types of references are used in `our release notes <#d-update-release-notes>`_, found under ``docs/src/release_notes/``.

The `reference to the header <#reference-to-header>`_ is used as links within the release notes' section headers for each package.

The `start & end references <#start-end-reference-labels>`_ are used within each package's section to pull in the relevant changelog information.


Aside: Populating the Long Description
**************************************

A Python package has the ability to provide a "long description" (the ``long_description`` keyword in a package's ``setup.py::setup()`` function).

PyPI refers to this value and uses it as the description for the project's landing page (e.g. `the project description for klio <https://pypi.org/project/klio/>`_).

All Klio packages programmatically by reading two documents: the package's ``README.rst`` (there's one in each ``<pkg>/`` directory), and parsing the changelog for the most recent version entry.
The logic can be found in a package's ``setup.py`` under ``get_long_description``.


Full Template Example
^^^^^^^^^^^^^^^^^^^^^

.. code-block:: rst

    Changelog
    =========

    .. _core-21.3.0:

    21.3.0 (UNRELEASED)
    -------------------

    .. start-21.3.0

    Changed
    *******

    * ``antigravity`` submodule is now located under ``flying_pigs`` module with a redirect from the original ``frozen_hell.antigravity`` location.

    Added
    *****

    * Added support for an invisibility cloak (See `KEP 999 <https://docs.klio.io/en/latest/keps/kep-999.html>`_).

    .. end-21.3.0

    .. _core-21.2.0:

    21.2.0 (2021-02-13)
    -------------------

    .. start-21.2.0

    No changes - bump version to sync with 21.2.0 release.

    .. end-21.2.0

    .. _core-21.1.0:

    21.1.0 (2021-01-01)
    -------------------

    .. start-21.1.0

    Fixed
    *****

    * Turning on flying mode now correctly detects elevation (See `PR 99999 <https://github.com/spotify/klio/pull/99999>`_).

    Removed
    *******

    * Deprecated support for Python 2.9
    * Removed unused ``nightvision`` dependency.

    .. end-21.1.0

----

*Credit: These instructions are heavily inspired by* |this write up|_.

.. it's impossible to nest formatting - aka have a link inside an italicized line. So this is a hack from https://stackoverflow.com/questions/4743845/format-text-in-a-link-in-restructuredtext

.. |this write up| replace:: *this write up*
.. _this write up:  https://hynek.me/articles/sharing-your-labor-of-love-pypi-quick-and-dirty/

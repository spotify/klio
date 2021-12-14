.. Copy & paste this template for each new general release.
    Do not create a new release note for pre-releases.
    See RELEASING.rst for more details.
    Feel free to remove these comments (prefaced by `.. `).

.. Add the new file name to ./index.rst in reverse-chrono order.

.. Update values in <BRACKETS>.

<YY.MM.build>
=============

<Section>
---------
.. Add any major user-facing notes on changes, additions, deprecations, etc.
    Have as many sections as needed.
    ./21.8.0.rst is a good example to refer to.

.. These sections aren't needed if no major user-facing changes.
    See ./21.9.0.rst as an example of a small general release.

Changes
-------

.. required: A "Changes" section is required. 
    Update  '<YY.MM.build> ' values with the release.

    This will automatically pull in the prose from each package's respective
    changelog as well as link to it.

    Add sub-sections for ``klio-audio`` and/or ``klio-devtools`` if they're 
    included in the release.

:ref:`klio-cli cli-<YY.MM.build>`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. include:: /reference/cli/changelog.rst
    :start-after: start-<YY.MM.build>
    :end-before: end-<YY.MM.build>

:ref:`klio lib-<YY.MM.build>`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. include:: /reference/lib/changelog.rst
    :start-after: start-<YY.MM.build>
    :end-before: end-<YY.MM.build>


:ref:`klio-core core-<YY.MM.build>`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. include:: /reference/core/changelog.rst
    :start-after: start-<YY.MM.build>
    :end-before: end-<YY.MM.build>

:ref:`klio-exec exec-<YY.MM.build>`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. include:: /reference/executor/changelog.rst
    :start-after: start-<YY.MM.build>
    :end-before: end-<YY.MM.build>


.. optional: use the ``.. ruberic:: Footnotes`` to add any footnote links.
    This creates a pretty section on the bottom of the page.
    See ./21.8.0.rst and https://docs.klio.io/en/stable/release_notes/21.8.0.html
    as an example
.. rubric:: Footnotes
# Copyright 2020 Spotify AB
"""
Custom Sphinx extension that defines a ``collapsible`` admonition.  This
directive will look like a normal admonition with the ability to toggle
its content to show/hide.

Example:

.. collapsible:: A title for my admonition

    Some body text. It can include other directives within and will be
    parsed like other admonitions, e.g.:

    .. code-block:: python

        print("I should be properly syntax highlighted")
"""

from docutils.parsers.rst.directives import admonitions
from sphinx.util import docutils


class CollapsibleAdmonition(admonitions.Admonition, docutils.SphinxDirective):
    """Add CSS classes to `.. collapsible::` admonition."""
    def run(self):
        if not self.options.get("class"):
            self.options["class"] = ["admonition-collapsible"]
        else:
            self.options["class"].append("admonition-collapsible")
        return super().run()


def setup(app):
    app.add_directive("collapsible", CollapsibleAdmonition)

    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }

# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import codecs
import os
import re

import sys
sys.path.append(os.path.abspath("./_ext"))

# -- Helper funcs

def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, *parts), "rb", "utf-8") as f:
        return f.read()


def find_version(*file_paths):
    """
    Build a path from *file_paths* and search for a ``__version__``
    string inside.
    """
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]", version_file, re.M
    )
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")



# -- Project information -----------------------------------------------------

project = "klio"
copyright = "2020, Spotify AB"
author = "The klio developers"

versions = {
    "klio_cli_release": find_version("../../cli/src/klio_cli/__init__.py"),
    "klio_core_release": find_version("../../core/src/klio_core/__init__.py"),
    "klio_devtools_release": find_version("../../devtools/src/klio_devtools/__init__.py"),
    "klio_exec_release": find_version("../../exec/src/klio_exec/__init__.py"),
    "klio_release": find_version("../../lib/src/klio/__init__.py"),
    "klio_audio_release": find_version("../../audio/src/klio_audio/__init__.py"),
}

# Define ``rst_epilog`` to make variables globally-available to compiled .rst files
rst_epilog = """
.. |klio-cli-version| replace:: {klio_cli_release}
.. |klio-version| replace:: {klio_release}
.. |klio-audio-version| replace:: {klio_release}
.. |klio-exec-version| replace:: {klio_exec_release}
.. |klio-core-version| replace:: {klio_core_release}
.. |klio-devtools-version| replace:: {klio_devtools_release}
""".format(**versions)

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.todo",  # highlight TODO items
    "sphinx.ext.intersphinx",  # interlink between other projects w/ sphinx docs
    "sphinx.ext.autodoc",  # auto-generate docs from docstrings
    "sphinx.ext.napoleon",  # handle Google-style docstrings
    "sphinx.ext.autosummary",  # auto-gen summaries
    "collapsible_admon",  # custom extension from _ext dir
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []

# sphinx-build has a "nitpick" mode (used during CI docs workflow and
# `make stricthtml`). We inherit some docs from Apache Beam, and some of
# their docstrings fail to compile in nitpick mode.
# https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-nitpick_ignore
nitpick_ignore = [
    # <-- nitpick docstrings from beam.io.gcp.WriteToBigQuery
    ("py:class", "callable"),
    ("py:class", "ValueProvider"),
    ("py:class", "apache_beam.io.gcp.internal.clients.bigquery.bigquery_v2_messages.TableSchema"),
    ("py:class", "BigQueryDisposition"),
    ("py:attr", "BigQueryDisposition.CREATE_IF_NEEDED"),
    ("py:attr", "BigQueryDisposition.CREATE_NEVER"),
    ("py:attr", "BigQueryDisposition.WRITE_TRUNCATE"),
    ("py:attr", "BigQueryDisposition.WRITE_APPEND"),
    ("py:attr", "BigQueryDisposition.WRITE_EMPTY"),
    # -->
    # <-- nitpick docstrings from beam.io.textio.WriteToText
    ("py:class", "WriteToText"),
    ("py:class", "apache_beam.io.filesystem.CompressionTypes.AUTO"),
    # -->
    # <-- nitpick drom beam.io.ReadFromText
    ("py:class", "ReadFromText"),
    # -->
]

# sphinx-build -b linkcheck will error out if links in docstrings are broken,
# including inherited docstrings (i.e. Beam)
# https://www.sphinx-doc.org/en/master/usage/configuration.html#confval-linkcheck_ignore
linkcheck_ignore = [
    r"https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs#configuration.load",
]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "pydata_sphinx_theme"
html_logo = "_static/images/shitty_logo.png"
html_theme_options = {
  "github_url": "https://github.com/spotify/klio",
  "twitter_url": "https://twitter.com/SpotifyEng",
  "show_prev_next": False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
html_css_files = ["css/custom.css", "css/colors.css"]  # relative to _static
html_js_files = ["js/custom.js"]  # relative to _static
html_favicon = "_static/images/k_favicon.png"
pygments_style = "vs"

# -- Extention configuration

# TODO ext: https://www.sphinx-doc.org/en/master/usage/extensions/todo.html
todo_include_todos = True
todo_emit_warnings = False
todo_link_only = False


# -- Autodoc config
autodoc_default_options = {
    "inherited-members": False,
}


# -- intersphinx mapping
# This will auto-generate links to Python's docs when referenced (e.g.
# :func:`pdb.set_trace` will link to the set_trace docs)
intersphinx_mapping = {
    "https://docs.python.org/3": None,
    "https://beam.apache.org/releases/pydoc/current": None,
}

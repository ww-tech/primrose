# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys

# sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/base"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/cleanup"))
sys.path.insert(
    0, os.path.abspath(os.path.dirname(__file__) + "/primrose/conditionalpath")
)
sys.path.insert(
    0, os.path.abspath(os.path.dirname(__file__) + "/primrose/configuration")
)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/dag"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/dataviz"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/models"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/pipelines"))
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/readers"))
sys.path.insert(
    0, os.path.abspath(os.path.dirname(__file__) + "/primrose/transformers")
)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__) + "/primrose/writers"))
sys.path.insert(
    0, os.path.abspath(os.path.dirname(__file__) + "/primrose/notifications")
)


# -- Project information -----------------------------------------------------

project = "primrose"
copyright = "2019, WW International, Inc"
author = "Carl Anderson, Michael Skarlinski"

# The full version, including alpha/beta/rc tags
release = "1.0.0"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "myst_parser",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True

autosummary_generate = True

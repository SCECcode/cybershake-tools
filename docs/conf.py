"""Sphinx configuration for the CyberShake Tools documentation."""

from click import Context
from sphinx.application import Sphinx

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


project = "CyberShake Tools"
copyright = "2021, University of Southern California"
author = "Rajiv Mayani"
release = "2026.10.0"
repository_url = "https://github.com/SCECcode/cybershake-tools"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",
    "sphinx_click.ext",
    "sphinx_copybutton",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "_includes", "Thumbs.db", ".DS_Store", "AGENTS.md"]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
# source_suffix = ".rst"
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
# https://sphinx-book-theme.readthedocs.io/en/stable/reference.html

html_theme = "sphinx_book_theme"
html_logo = "_static/assets/images/logo-scec.jpg"
html_favicon = "_static/assets/images/logo-scec.jpg"
html_title = "CyberShake Tools"
html_static_path = ["_static"]
html_theme_options = {
    "path_to_docs": "docs",
    "repository_url": repository_url,
    "repository_branch": "master",
    "use_edit_page_button": True,
    "use_source_button": True,
    "use_issues_button": True,
    # "use_repository_button": True,
    "use_download_button": True,
    "use_sidenotes": True,
    "show_toc_level": 2,
    # "announcement": "⚠️ Announcement",
    "logo": {
        "image_light": "_static/assets/images/logo-scec.jpg",
        "image_dark": "_static/assets/images/logo-scec.jpg",
        "alt_text": "Kiso - Home",
        # "text": html_title,  # Uncomment to try text with logo
    },
    "icon_links": [
        {
            "name": "GitHub",
            "url": repository_url,
            "icon": "fa-brands fa-github",
        },
        {
            "name": "PyPI",
            "url": "https://pypi.org/project/cybershake-tools/",
            "icon": "https://img.shields.io/pypi/dw/cybershake-tools",
            "type": "url",
        },
    ],
}
jsonschema_options = {
    "lift_title": True,
    "lift_description": True,
    "lift_definitions": True,
    "auto_target": True,
    "auto_reference": True,
}


def _hide_click_api_details(_app: Sphinx, ctx: Context, lines: list[str]) -> None:
    """Remove callback API details hidden from normal Click help."""
    if "\f" in (ctx.command.help or "") and "Args:" in lines:
        del lines[lines.index("Args:") :]


def setup(app: Sphinx) -> None:
    """Register documentation-build callbacks."""
    app.connect("sphinx-click-process-description", _hide_click_api_details)

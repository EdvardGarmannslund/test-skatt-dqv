"""Sphinx configuration."""
project = "Test Skatt Dqv"
author = "Edvard Garmannslund"
copyright = "2025, Edvard Garmannslund"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
]
autodoc_typehints = "description"
html_theme = "furo"

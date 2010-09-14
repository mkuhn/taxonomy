"""
Create unix package:    python setup.py sdist
"""

from distutils.core import setup
import os
import sys
import glob

from __init__ import __version__

params = {'author': 'Noah Hoffman',
          'author_email': 'ngh2@uw.edu',
          'description': 'Tools for manipulating biological taxonomies.',
          'name': 'Taxonomy',
          'package_dir': {'Taxonomy': '.'},
          'packages': ['Taxonomy'],
          'scripts': glob.glob('scripts/*.py'),
          # 'package_data':{'taxonomy': glob.glob('data/*')},
          'url': 'http://web.labmed.washington.edu/nhoffman',
          'version': __version__}

setup(**params)


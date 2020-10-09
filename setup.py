#!/usr/bin/env python3
from setuptools import setup, find_packages
import re
import os

version = re.search("__version__ = '([^']+)'", open(
    os.path.join(os.path.dirname(__file__), 'pygdrive/__init__.py')
).read().strip()).group(1)

setup(
    name='pygdrive',
    version=version,
    author="Tatsuo Nakajyo",
    author_email="tnak@nekonaq.com",
    license='BSD',
    packages=find_packages(),
    python_requires='~=3.6.9',
    install_requires=[
        'python-dateutil',
        'pygsheets~=2.0.3',
    ],
)

# Local Variables:
# compile-command: "python3 ./setup.py sdist"
# End:

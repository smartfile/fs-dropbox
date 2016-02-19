#!/usr/bin/env python

import os
from setuptools import setup

name = 'dropboxfs'
version = '0.4.2'
release = '0'
versrel = version + '-' + release
readme = os.path.join(os.path.dirname(__file__), 'README.rst')
long_description = file(readme).read()

setup(
    name=name,
    version=versrel,
    description='A PyFilesystem backend for the Dropbox API.',
    long_description=long_description,
    requires=[
        'fs',
        'dropbox',
    ],
    author='SmartFile',
    author_email='tcunningham@smartfile.com',
    maintainer='Travis Cunningham',
    maintainer_email='tcunningham@smartfile.com',
    url='http://github.com/smartfile/fs-dropbox/',
    license='GPLv3',
    py_modules=['dropboxfs'],
    package_data={'': ['README.rst']},
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ),
)

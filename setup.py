#!/usr/bin/env python

import re
import sys

from setuptools import setup


def get_version(filename):
    # This regex isn't very robust, but it should work for most files.
    regex = re.compile(r'''__version__.*=.*['"](\d+\.\d+(?:\.\d+)?)['"]''')
    with open(filename) as f:
        for line in f:
            match = regex.search(line)
            if match:
                return match.group(1)


setup(name="mrs-mapreduce",
    version=get_version('mrs/version.py'),
    description="A lightweight MapReduce implementation for computationally"
        " intensive programs",
    #long_description="See README",
    license="GNU GPL",
    author="BYU AML Lab",
    author_email="mrs-mapreduce@googlegroups.com",
    url="http://code.google.com/p/mrs-mapreduce/",
    packages=['mrs'],
    classifiers=['Development Status :: 4 - Beta',
                'Operating System :: POSIX :: Linux',
                'Environment :: Console',
                'Intended Audience :: Science/Research',
                'License :: OSI Approved :: GNU General Public License (GPL)',
                'Natural Language :: English',
                'Programming Language :: Python',
                'Programming Language :: Python :: 2',
                'Programming Language :: Python :: 2.6',
                'Programming Language :: Python :: 2.7',
                'Programming Language :: Python :: 3',
                'Programming Language :: Python :: 3.1',
                'Programming Language :: Python :: 3.2',
                'Topic :: Scientific/Engineering :: Information Analysis'],
    )

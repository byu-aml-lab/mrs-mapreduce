#!/usr/bin/env python
# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    license="Apache License, Version 2.0",
    author="Andrew McNabb",
    author_email="mrs-mapreduce@googlegroups.com",
    url="http://code.google.com/p/mrs-mapreduce/",
    packages=['mrs'],
    classifiers=['Development Status :: 4 - Beta',
                'Operating System :: POSIX :: Linux',
                'Environment :: Console',
                'Intended Audience :: Science/Research',
                'License :: OSI Approved :: Apache Software License',
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

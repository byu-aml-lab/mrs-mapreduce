#!/usr/bin/env python

from setuptools import setup
from mrs import version

setup(name="MapReduce Simplified",
      version=version.VERSION,
      description="Mrs: A simplified MapReduce implimentation in Python",
      long_description="See README",
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
                   'Topic :: Scientific/Engineering :: Information Analysis'],
     )

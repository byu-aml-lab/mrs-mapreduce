#!/usr/bin/env python

# Copyright 2008 Brigham Young University
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# Mrs.  If not, see <http://www.gnu.org/licenses/>.
#
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

import io
from mapreduce import Implementation, mrs_map, mrs_reduce, Job
from util import try_makedirs

def run_mockparallel(registry, user_run, args, opts):
    # Set up job directory
    shared_dir = opts.mrs_shared
    from util import try_makedirs
    try_makedirs(shared_dir)
    import tempfile
    jobdir = tempfile.mkdtemp(prefix='mrs.job_', dir=shared_dir)

    # Create Job
    job = Job(registry, jobdir, user_run, args, opts)

    # TODO: later, don't assume that this is a short-running function:
    job.run()

    # TODO: this should spin off as another thread while job runs in the
    # current thread:
    mrs_exec = MockParallel(job, registry)
    mrs_exec.run()
    return 0


def run_serial(registry, run, inputs, output, options):
    """Mrs Serial
    """
    # Create Job
    job = Job(registry, jobdir, user_run, args, opts)

    # TODO: later, don't assume that this is a short-running function:
    job.run()

    # TODO: this should spin off as another thread while job runs in the
    # current thread:
    mrs_exec = Serial(job, registry)
    mrs_exec.run()
    return 0


# TODO: rewrite Serial implementation to use job and to be more general
class Serial(Implementation):
    """MapReduce execution on a single processor
    """
    def __init__(self, job, registry, **kwds):
        Implementation.__init__(self, **kwds)
        self.job = job
        self.registry = registry

    def run(self):
        """Run a MapReduce operation in serial.
        """
        from itertools import chain
        # MAP PHASE
        input_files = [io.openfile(filename) for filename in self.inputs]
        all_input = chain(*input_files)
        map_output = mrs_map(registry['mapper'], all_input)

        # SORT PHASE
        import operator
        interm = sorted(map_output, key=operator.itemgetter(0))

        # REDUCE PHASE
        output_file = io.openfile(self.output, 'w')
        for k, v in mrs_reduce(registry['reducer'], interm):
            output_file.write(k, v)

        # cleanup
        for f in input_files:
            f.close()
        output_file.close()


class MockParallel(Implementation):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, job, registry, **kwds):
        Implementation.__init__(self, **kwds)
        self.job = job
        self.registry = registry

    def run(self):
        import sys, os

        job = self.job

        # Run Tasks:
        for task in iter(job.get_task, None):
            task.active()
            job.print_status()
            task.run()
            task.finished(task.outurls)

# vim: et sw=4 sts=4

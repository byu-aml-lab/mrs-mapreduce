#!/usr/bin/env python

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

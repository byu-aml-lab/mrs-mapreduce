#!/usr/bin/env python

# TODO: right now we assume that input files are pre-split.

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

import threading

def mrs_simple(job, args, opts):
    """Default run function for a map phase and reduce phase"""
    if len(args) < 2:
        import sys
        print >>sys.stderr, "Requires input(s) and an output."
        sys.exit(-1)
    source = job.file_data(args[:-1])
    intermediate = job.map_data(source, 'mapper')
    output = job.reduce_data(intermediate, 'reducer', outdir=args[-1])


# TODO: make everything reentrant once Parallel runs in a different thread.
class Job(object):
    """Keep track of all operations that need to be performed."""
    def __init__(self, registry, jobdir, user_run, args, opts):
        self.datasets = []
        self.current = 0
        self.registry = registry
        self.jobdir = jobdir
        self.user_run = user_run
        self.args = args
        self.opts = opts

        self.default_reduce_parts = 1
        self.default_reduce_tasks = opts.reduce_tasks

    def run(self):
        job = self
        self.user_run(job, self.args, self.opts)

    def file_data(self, filenames):
        from datasets import FileData
        ds = FileData(filenames)
        return ds

    def map_data(self, input, mapper, nparts=None, outdir=None, parter=None):
        """Define a set of data computed with a map operation.

        Specify the input dataset and a mapper function.  The mapper must be
        in the job's registry and may be specified as a name or function.
        """
        if nparts is None:
            nparts = self.default_reduce_tasks

        from datasets import MapData
        if outdir is None:
            outdir = self.jobdir
        ds = MapData(input, mapper, nparts, outdir, parter=parter,
                registry=self.registry)
        self.datasets.append(ds)
        return ds

    def reduce_data(self, input, reducer, nparts=None, outdir=None,
            parter=None):
        """Define a set of data computed with a reducer operation.

        Specify the input dataset and a reducer function.  The reducer must be
        in the job's registry and may be specified as a name or function.
        """
        if nparts is None:
            nparts = self.default_reduce_parts

        from datasets import ReduceData
        if outdir is None:
            outdir = self.jobdir
        ds = ReduceData(input, reducer, nparts, outdir, parter=parter,
                registry=self.registry)
        self.datasets.append(ds)
        return ds

    def print_status(self):
        if self.done():
            print 'Done'
        else:
            ds = self.datasets[self.current]
            if not ds.tasks_made:
                ds.make_tasks()
            ds.print_status()

    def get_task(self):
        """Return the next available task"""
        N = len(self.datasets)
        while self.current < N:
            ds = self.datasets[self.current]
            if not ds.tasks_made:
                ds.make_tasks()
            if ds.ready():
                self.current += 1
            else:
                return ds.get_task()

    def done(self):
        return (self.current >= len(self.datasets))


class Implementation(threading.Thread):
    """Carries out the work.

    There are various ways to implement MapReduce:
    - serial execution on one processor
    - parallel execution on a shared-memory system
    - parallel execution with shared storage on a POSIX filesystem (like NFS)
    - parallel execution with a non-POSIX distributed filesystem

    To execute, make sure to do:
    job.inputs.append(input_filename)
    job.operations.append(mrs_operation)

    By the way, since Implementation inherits from threading.Thread, you can
    execute a MapReduce operation as a thread.  Slick, eh?
    """
    def __init__(self, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.inputs = []
        self.operations = []

    def add_input(self, input):
        """Add a filename to be used for input to the map task.
        """
        self.inputs.append(input)

    def run(self):
        raise NotImplementedError("I think you should have"
                " instantiated a subclass of Implementation.")


class Task(object):
    def __init__(self, taskid, input, outdir):
        self.taskid = taskid
        self.input = input
        self.outdir = outdir
        self.dataset = None

        self.output = None
        self._outurls = []

        # TODO: check to see if there's somewhere better for this:
        from util import try_makedirs
        try_makedirs(outdir)

    def active(self):
        self.dataset.task_started(self)

    def finished(self, urls):
        self._outurls = urls
        self.dataset.task_finished(self)

    def canceled(self):
        self.dataset.task_canceled(self)

    def inurls(self):
        return [bucket.url for bucket in self.input[:, self.taskid]]

    def outurls(self):
        # Normally, there's an output object, but the master only holds a
        # list of urls.
        if self.output:
            return [bucket.url for bucket in self.output]
        else:
            return self._outurls


class MapTask(Task):
    def __init__(self, taskid, input, registry, map_name, part_name, outdir,
            nparts, **kwds):
        Task.__init__(self, taskid, input, outdir)
        self.map_name = map_name
        self.mapper = registry[map_name]
        self.part_name = part_name
        if part_name == '':
            self.partition = default_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self):
        import datasets
        from itertools import chain
        import tempfile
        self.input.fetchall()
        all_input = self.input.itersplit(0)

        # PREP
        subdirbase = "map_%s_" % self.taskid
        directory = tempfile.mkdtemp(dir=self.outdir, prefix=subdirbase)
        self.output = datasets.Output(self.partition, self.nparts,
                taskid=self.taskid, directory=directory)

        self.output.collect(mrs_map(self.mapper, all_input))
        self.output.dump()

        self.input.close()


# TODO: allow configuration of output format
# TODO: make more like MapTask (part_name, nparts, etc.)
class ReduceTask(Task):
    def __init__(self, taskid, input, registry, reduce_name, outdir,
            format='txt', **kwds):
        Task.__init__(self, taskid, input, outdir)
        self.reduce_name = reduce_name
        self.reducer = registry[reduce_name]
        self.format = format

    def run(self):
        import io
        from itertools import chain

        # PREP
        import io
        import tempfile
        subdirbase = "reduce_%s_" % self.taskid
        directory = tempfile.mkdtemp(dir=self.outdir, prefix=subdirbase)
        self.output = io.Output(None, 1, directory=directory,
                format=self.format)

        # SORT PHASE
        inputfiles = [io.openfile(url) for url in self.inputurls()]
        all_input = sorted(chain(*inputfiles))

        # Do the following if external sort is necessary (i.e., the input
        # files are too big to fit in memory):
        #fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        #os.close(fd)
        #io.hexfile_sort(interm_names, sorted_name)

        # REDUCE PHASE
        self.output.collect(mrs_reduce(self.reducer, all_input))
        self.output.savetodisk()

        for f in inputfiles:
            f.close()
        self.output.close()


def default_partition(x, n):
    return hash(x) % n

def mrs_map(mapper, input):
    """Perform a map from the entries in input."""
    for inkey, invalue in input:
        for key, value in mapper(inkey, invalue):
            yield (key, value)

def grouped_read(input_file):
    """An iterator that yields key-iterator pairs over a sorted input_file.

    This is very similar to itertools.groupby, except that we assume that the
    input_file is sorted, and we assume key-value pairs.
    """
    input_itr = iter(input_file)
    input = input_itr.next()
    next_pair = list(input)

    def subiterator():
        # Closure warning: don't rebind next_pair anywhere in this function
        group_key, value = next_pair

        while True:
            yield value
            try:
                input = input_itr.next()
            except StopIteration:
                next_pair[0] = None
                return
            key, value = input
            if key != group_key:
                # A new key has appeared.
                next_pair[:] = key, value
                return

    while next_pair[0] is not None:
        yield next_pair[0], subiterator()
    raise StopIteration


def mrs_reduce(reducer, input):
    """Perform a reduce from the entries in input into output.

    A reducer is an iterator taking a key and an iterator over values for that
    key.  It yields values for that key.
    """
    for key, iterator in grouped_read(input):
        for value in reducer(key, iterator):
            yield (key, value)

# vim: et sw=4 sts=4

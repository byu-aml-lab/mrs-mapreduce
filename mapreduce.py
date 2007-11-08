#!/usr/bin/env python

# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

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


def simple_run(job, input, registry, map_name, part_name, reduce_name,
        map_tasks=1, reduce_tasks=1):
    map_out = job.map_data(input, registry, map_name, part_name, map_tasks,
            reduce_tasks)
    reduce_out = job.reduce_data(map_out, registry, map_name, part_name,
            map_tasks, reduce_tasks)


class Job(object):
    """Keep track of all operations that need to be performed."""
    def __init__(self):
        self.datasets = []

    def map_data(self, *args):
        ds = MapData(*args)
        self.datasets.append(ds)
        return ds

    def reduce_data(self, *args):
        ds = ReduceData(*args)
        self.datasets.append(ds)
        return ds


# maybe this should be ParallelDataSet:
class DataSet(object):
    """Manage input to or output from a map or reduce operation.
    
    The data are evaluated lazily.  A DataSet knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.
    """
    def __init__(self, input, registry, func_name, part_name, ntasks=1,
            nparts=1):
        self.input = input
        self.registry = registry
        self.map_name = map_name
        self.part_name = part_name
        self.ntasks = ntasks
        self.nparts = nparts

        # TODO: store a mapping from tasks to hosts and a map from hosts to
        # tasks.  This way you can know where to find data.  You also know
        # which hosts to restart in case of failure.


class MapData(DataSet):
    def __init__(self, input, registry, map_name, part_name, ntasks, nparts):
        DataSet.__init__(self, input, registry, map_name, part_name, ntasks,
                nparts)


class ReduceData(DataSet):
    def __init__(self, input, registry, reduce_name, part_name, ntasks,
            nparts):
        DataSet.__init__(self, input, registry, reduce_name, part_name,
                ntasks, nparts)


# This needs to go away:
class Operation(object):
    """Specifies a map phase followed by a reduce phase."""
    def __init__(self, registry, run, map_tasks=1, reduce_tasks=1):
        self.registry = registry
        self.run = run
        self.map_tasks = map_tasks
        self.reduce_tasks = reduce_tasks


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


class MapTask(threading.Thread):
    def __init__(self, taskid, registry, map_name, part_name, jobdir,
            reduce_tasks, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.taskid = taskid
        self.map_name = map_name
        self.mapper = registry[map_name]
        self.part_name = part_name
        self.partition = registry[part_name]
        self.inputs = []
        self.jobdir = jobdir
        self.reduce_tasks = reduce_tasks

    def run(self):
        import io
        from itertools import chain
        import tempfile
        inputfiles = [io.openfile(filename) for filename in self.inputs]
        all_input = chain(*inputfiles)

        # PREP
        subdirbase = "map_%s_" % self.taskid
        directory = tempfile.mkdtemp(dir=self.jobdir, prefix=subdirbase)
        self.output = io.Output(self.partition, self.reduce_tasks,
                directory=directory)

        self.output.collect(mrs_map(self.mapper, all_input))
        self.output.savetodisk()

        for input_file in all_input:
            input_file.close()
        self.output.close()


# TODO: allow configuration of output format
class ReduceTask(threading.Thread):
    def __init__(self, taskid, registry, reduce_name, outdir, format='txt',
            **kwds):
        threading.Thread.__init__(self, **kwds)
        self.taskid = taskid
        self.reduce_name = reduce_name
        self.reducer = registry[reduce_name]
        self.outdir = outdir
        self.inputs = []
        self.output = None
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
        inputfiles = [io.openfile(filename) for filename in self.inputs]
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

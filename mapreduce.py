#!/usr/bin/env python

# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

import threading

class Program(object):
    """Mrs Program (mapper, reducer, etc.)"""
    def __init__(self, mapper, reducer, partition):
        self.mapper = mapper
        self.reducer = reducer
        self.partition = partition

class Operation(object):
    """Specifies a map phase followed by a reduce phase.
    
    The output_format is a file format, such as HexFile or TextFile.
    """
    def __init__(self, mrs_prog, map_tasks=1, reduce_tasks=1,
            output_format=None):
        self.mrs_prog = mrs_prog
        self.map_tasks = map_tasks
        self.reduce_tasks = reduce_tasks

        if output_format is None:
            import io
            self.output_format = io.TextFile
        else:
            self.output_format = output_format


class Job(threading.Thread):
    """Keeps track of the parameters of the MR job and carries out the work.

    There are various ways to implement MapReduce:
    - serial execution on one processor
    - parallel execution on a shared-memory system
    - parallel execution with shared storage on a POSIX filesystem (like NFS)
    - parallel execution with a non-POSIX distributed filesystem

    To execute, make sure to do:
    job.inputs.append(input_filename)
    job.operations.append(mrs_operation)

    By the way, since Job inherits from threading.Thread, you can execute a
    MapReduce operation as a thread.  Slick, eh?
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
        raise NotImplementedError(
                "I think you should have instantiated a subclass of Job.")

def map_filename(taskid):
    """Filename for the directory for intermediate output from taskid.
    """
    return "map_%s_" % taskid

def reduce_filename(taskid):
    """Filename for the directory for output from taskid.
    """
    return "reduce_%s_" % taskid


class MapTask(threading.Thread):
    def __init__(self, taskid, mrs_prog, input, jobdir, reduce_tasks,
            **kwds):
        threading.Thread.__init__(self, **kwds)
        self.taskid = taskid
        self.mrs_prog = mrs_prog
        self.input = input
        self.jobdir = jobdir
        self.reduce_tasks = reduce_tasks


    def run(self):
        import io
        input_file = io.openfile(self.input)

        # PREP
        import tempfile
        import io
        subdirbase = map_filename(self.taskid)
        directory = tempfile.mkdtemp(dir=self.jobdir, prefix=subdirbase)
        self.output = io.Output(self.mrs_prog.partition, self.reduce_tasks,
                directory=directory)

        self.output.collect(mrs_map(self.mrs_prog.mapper, input_file))
        self.output.savetodisk()

        input_file.close()
        self.output.close()


# TODO: allow configuration of output format
class ReduceTask(threading.Thread):
    def __init__(self, taskid, mrs_prog, outdir, jobdir, format='txt', **kwds):
        threading.Thread.__init__(self, **kwds)
        self.taskid = taskid
        self.mrs_prog = mrs_prog
        self.outdir = outdir
        self.jobdir = jobdir
        self.inputs = []
        self.output = None
        self.format = format

    def run(self):
        import io
        from itertools import chain

        # PREP
        import io
        import tempfile
        subdirbase = reduce_filename(self.taskid)
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
        self.output.collect(mrs_reduce(self.mrs_prog.reducer, all_input))
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

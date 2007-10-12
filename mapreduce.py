#!/usr/bin/env python

# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

import threading

class Operation(object):
    """Specifies a map phase followed by a reduce phase.
    
    The output_format is a file format, such as HexFile or TextFile.
    """
    def __init__(self, mapper, reducer, partition, map_tasks=1, reduce_tasks=1,
            output_format=None):
        self.mapper = mapper
        self.reducer = reducer
        self.partition = partition
        self.map_tasks = map_tasks
        self.reduce_tasks = reduce_tasks

        if output_format is None:
            import formats
            self.output_format = formats.TextFile
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

class MapTask(threading.Thread):
    def __init__(self, taskid, mapper, partition, input, reduce_tasks,
            interm_prefix, **kwds):
        threading.Thread.__init__(self, **kwds)
        self.taskid = taskid
        self.mapper = mapper
        self.partition = partition
        self.input = input
        self.reduce_tasks = reduce_tasks
        self.interm_prefix = interm_prefix

        self.workers = []

    def assign(self, worker):
        self.workers.append(worker)

    def remove(self, worker):
        self.workers.remove(worker)

    def run(self):
        input_format = formats.fileformat(self.input)
        input_file = input_format(open(self.input))

        # create a new interm_name for each reducer
        interm_dirs = [self.interm_prefix + str(i)
                for i in xrange(reduce_tasks)]
        interm_filenames = [os.path.join(d, 'from_%s.hexfile' % self.taskid)
                for d in interm_dirs]
        interm_files = [formats.HexFile(open(name, 'w'))
                for name in interm_filenames]

        map(self.mapper, self.partition, input_file, interm_files)

        input_file.close()
        for f in interm_files:
            f.close()

    def __cmp__(self, other):
        # TODO: make this much more complex and robust
        if isinstance(other, MapTask):
            return cmp(len(self.workers), len(other.workers))
        elif isinstance(other, ReduceTask):
            return -1
        else:
            raise NotImplementedError

class ReduceTask:
    pass

def default_partition(x, n):
    return hash(x) % n

def mrs_map(mapper, input_file, output_files, partition=None):
    """Perform a map from the entries in input_file into output_files.

    If partition is None, output_files should be a single file.  Otherwise,
    output_files is a list, and partition is a function that takes a key and
    returns the index of the file in output_files to which that key should be
    written.
    """
    if partition is not None:
        N = len(output_files)
    while True:
        try:
            input = input_file.next()
            if partition is None:
                for key, value in mapper(*input):
                    output_files.write(key, value)
            else:
                for key, value in mapper(*input):
                    index = partition(key, N)
                    output_files[index].write(key, value)
        except StopIteration:
            return

def grouped_read(input_file):
    """An iterator that yields key-iterator pairs over a sorted input_file.

    This is very similar to itertools.groupby, except that we assume that the
    input_file is sorted, and we assume key-value pairs.
    """
    input = input_file.next()
    next_pair = list(input)

    def subiterator():
        # Closure warning: don't rebind next_pair anywhere in this function
        group_key, value = next_pair

        while True:
            yield value
            try:
                input = input_file.next()
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


def mrs_reduce(reducer, input_file, output_file):
    """Perform a reduce from the entries in input_file into output_file.

    A reducer is an iterator taking a key and an iterator over values for that
    key.  It yields values for that key.  Optionally, the reducer can have a
    switch_keys attribute set to True, in which case it yields key-value
    pairs rather than just values.
    """
    try:
        switch_keys = reducer.switch_keys
    except AttributeError:
        switch_keys = False

    for key, iterator in grouped_read(input_file):
        if switch_keys:
            for new_key, value in reducer(key, iterator):
                output_file.write(new_key, value)
        else:
            for value in reducer(key, iterator):
                output_file.write(key, value)

# vim: et sw=4 sts=4

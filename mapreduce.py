#!/usr/bin/env python

from hexfile import HexFile, sort
from textfile import TextFile

# TODO: make it so we can output from map into a different file for each
# reducer.
# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

class Operation(object):
    """Specifies a map phase followed by a reduce phase."""
    pass

class Job(object):
    """Keeps track of the parameters of the MR job and carries out the work.

    There are various ways to implement MapReduce:
    - serial execution on one processor
    - parallel execution on a shared-memory system
    - parallel execution with shared storage on a POSIX filesystem (like NFS)
    - parallel execution with a non-POSIX distributed filesystem
    """
    def __init__(self):
        self.operations = []


class SerialJob(Job):
    """MapReduce execution on a single processor
    """
    pass

class POSIXJob(Job):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, shared_dir):
        Job.__init__(self)
        self.shared_dir = shared_dir

def mapreduce(mapper, reducer, input_filename):
    """Serial mapreduce for filename input."""
    input_file = TextFile(input_filename)
    intermediate_file = HexFile('intermediate1.txt', 'w')

    map(mapper, input_file, intermediate_file)
    input_file.close()
    intermediate_file.close()

    sort('intermediate1.txt', 'intermediate2.txt')

    intermediate_file = HexFile('intermediate2.txt')
    output_file = TextFile('output.txt', 'w')

    reduce(reducer, intermediate_file, output_file)

    intermediate_file.close()
    output_file.close()

def map(mapper, input_file, output_file):
    """Perform a map from the entries in input_file into output_file.
    """
    while True:
        try:
            input = input_file.next()
            for key, value in mapper(*input):
                output_file.write(key, value)
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


def reduce(reducer, input_file, output_file):
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

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

import hexfile, textfile

# TODO: make it so we can output from map into a different file for each
# reducer.
# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

class Operation(object):
    """Specifies a map phase followed by a reduce phase.
    
    The output_format is a file format, such as HexFile or TextFile.
    """
    def __init__(self, mapper, reducer, output_format=None,
            output_dir='.', output_prefix=None):
        self.mapper = mapper
        self.reducer = reducer
        self.output_prefix = output_prefix
        self.output_dir = output_dir
        if output_format is None:
            self.output_format = textfile.TextFile
        else:
            self.output_format = output_format


class Job(object):
    """Keeps track of the parameters of the MR job and carries out the work.

    There are various ways to implement MapReduce:
    - serial execution on one processor
    - parallel execution on a shared-memory system
    - parallel execution with shared storage on a POSIX filesystem (like NFS)
    - parallel execution with a non-POSIX distributed filesystem

    To execute, make sure to do:
    job.inputs.append(input_filename)
    job.operations.append(mrs_operation)
    """
    def __init__(self):
        self.inputs = []
        self.operations = []

    def add_input(self, input):
        """Add a filename to be used for input to the map task.
        """
        self.inputs.append(input)

    def run(self):
        raise NotImplementedError(
                "I think you should have instantiated a subclass of Job.")


class SerialJob(Job):
    """MapReduce execution on a single processor
    """
    def __init__(self):
        Job.__init__(self)

    def run(self, debug=False):
        """Run a MapReduce operation in serial.
        
        If debug is specified, don't cleanup temporary files afterwards.
        """
        import os, tempfile

        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        operation = self.operations[0]

        if len(self.inputs) != 1:
            raise NotImplementedError("Requires exactly one input file.")
        input = self.inputs[0]

        # MAP PHASE
        input_file = textfile.TextFile(open(input))
        fd, intermediate_name = tempfile.mkstemp(prefix='mrs.intermediate_')
        intermediate_tmp = os.fdopen(fd, 'w')
        intermediate_file = hexfile.HexFile(intermediate_tmp)

        map(operation.mapper, input_file, intermediate_file)

        input_file.close()
        intermediate_file.close()

        # SORT PHASE
        fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        os.close(fd)
        hexfile.sort(intermediate_name, sorted_name)

        # REDUCE PHASE
        sorted_file = hexfile.HexFile(open(sorted_name))
        output_prefix = operation.output_prefix
        if output_prefix is None:
            output_prefix = 'mrs.output_'
        fd, output_name = tempfile.mkstemp(prefix=output_prefix,
                dir=operation.output_dir)
        output_tmp = os.fdopen(fd, 'w')
        output_file = operation.output_format(output_tmp)

        reduce(operation.reducer, sorted_file, output_file)

        sorted_file.close()
        output_file.close()

        # CLEANUP

        if not debug:
            import os
            os.unlink(intermediate_name)
            os.unlink(sorted_name)

        return [output_name]


class POSIXJob(Job):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, shared_dir):
        Job.__init__(self)
        self.shared_dir = shared_dir

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

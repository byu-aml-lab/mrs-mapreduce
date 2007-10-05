#!/usr/bin/env python

import hexfile, textfile

# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

class Operation(object):
    """Specifies a map phase followed by a reduce phase.
    
    The output_format is a file format, such as HexFile or TextFile.
    """
    def __init__(self, mapper, reducer, map_tasks=1, reduce_tasks=1,
            input_format=None, output_format=None):
        self.mapper = mapper
        self.reducer = reducer
        self.map_tasks = map_tasks
        self.reduce_tasks = reduce_tasks

        if input_format is None:
            self.input_format = textfile.TextFile
        else:
            self.input_format = input_format
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
    def __init__(self, inputs, output):
        Job.__init__(self)
        self.inputs = inputs
        self.output = output

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
        fd, interm_name = tempfile.mkstemp(prefix='mrs.interm_')
        interm_tmp = os.fdopen(fd, 'w')
        interm_file = hexfile.HexFile(interm_tmp)

        map(operation.mapper, input_file, interm_file)

        input_file.close()
        interm_file.close()

        # SORT PHASE
        fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        os.close(fd)
        hexfile.sort(interm_name, sorted_name)

        # REDUCE PHASE
        sorted_file = hexfile.HexFile(open(sorted_name))
        output_file = operation.output_format(open(self.output, 'w'))

        reduce(operation.reducer, sorted_file, output_file)

        sorted_file.close()
        output_file.close()

        # CLEANUP

        if not debug:
            import os
            os.unlink(interm_name)
            os.unlink(sorted_name)

        return [output_name]


class POSIXJob(Job):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, inputs, output_dir, shared_dir, reduce_tasks=1):
        Job.__init__(self)
        self.inputs = inputs
        self.output_dir = output_dir
        self.shared_dir = shared_dir
        self.partition = default_partition

    def run(self, debug=False):
        import os
        from tempfile import mkstemp, mkdtemp

        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        operation = self.operations[0]

        map_tasks = operation.map_tasks
        if map_tasks != len(self.inputs):
            raise NotImplementedError("Requires exactly 1 map_task per input.")

        reduce_tasks = operation.reduce_tasks

        # PREP
        jobdir = mkdtemp(prefix='mrs.job_', dir=self.shared_dir)

        interm_path = os.path.join(jobdir, 'interm_')
        interm_dirs = [interm_path + str(i) for i in xrange(reduce_tasks)]
        for name in interm_dirs:
            os.mkdir(name)

        output_dir = os.path.join(jobdir, 'output')
        os.mkdir(output_dir)


        # MAP PHASE
        ## still serial
        for mapper_id, filename in enumerate(self.inputs):
            input_file = operation.input_format(open(filename))
            # create a new interm_name for each reducer
            interm_filenames = [os.path.join(d, 'from_%s' % mapper_id)
                    for d in interm_dirs]
            interm_files = [hexfile.HexFile(open(name, 'w'))
                    for name in interm_filenames]

            map(operation.mapper, input_file, interm_files,
                    partition=self.partition)

            input_file.close()
            for f in interm_files:
                f.close()

        for reducer_id in xrange(operation.reduce_tasks):
            # SORT PHASE
            interm_directory = interm_path + str(reducer_id)
            fd, sorted_name = mkstemp(prefix='mrs.sorted_')
            os.close(fd)
            interm_filenames = [os.path.join(interm_directory, s)
                    for s in os.listdir(interm_directory)]
            hexfile.sort(interm_filenames, sorted_name)

            # REDUCE PHASE
            sorted_file = hexfile.HexFile(open(sorted_name))
            basename = 'reducer_%s' % reducer_id
            output_name = os.path.join(self.output_dir, basename)
            output_file = operation.output_format(open(output_name, 'w'))

            reduce(operation.reducer, sorted_file, output_file)

            sorted_file.close()
            output_file.close()

        # CLEANUP

        #if not debug:
        #    import os
        #    os.unlink(interm_name)
        #    os.unlink(sorted_name)

        #return [output_name]



def default_partition(x, n):
    return hash(x) % n

def map(mapper, input_file, output_files, partition=None):
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

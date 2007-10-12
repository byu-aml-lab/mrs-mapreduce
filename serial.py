#!/usr/bin/env python

import hexfile, textfile
from mapreduce import Job, mrs_map, mrs_reduce

def run_posix(mapper, reducer, inputs, output, options):
    map_tasks = options.map_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1

    if options.map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    from mrs.mapreduce import Operation
    op = Operation(mapper, reducer, map_tasks=map_tasks,
            reduce_tasks=options.reduce_tasks)
    mrsjob = POSIXJob(inputs, output, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


def run_serial(mapper, reducer, inputs, output, options):
    """Mrs Serial
    """
    from mrs.mapreduce import Operation
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(inputs, output)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


# TODO: Since SerialJob is really just for debugging anyway, it might be a
# good idea to have another version that does all of the sorting in memory
# (without writing to an intermediate file) in addition to the current
# implementation that writes to an intermediate file and uses UNIX sort.
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

        mrs_map(operation.mapper, input_file, interm_file)

        input_file.close()
        interm_file.close()

        # SORT PHASE
        fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        os.close(fd)
        hexfile.sort(interm_name, sorted_name)

        # REDUCE PHASE
        sorted_file = hexfile.HexFile(open(sorted_name))
        output_file = operation.output_format(open(self.output, 'w'))

        mrs_reduce(operation.reducer, sorted_file, output_file)

        sorted_file.close()
        output_file.close()

        # CLEANUP

        if not debug:
            import os
            os.unlink(interm_name)
            os.unlink(sorted_name)


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

# vim: et sw=4 sts=4

#!/usr/bin/env python

import io
from mapreduce import Job, mrs_map, mrs_reduce, MapTask, ReduceTask
from util import try_makedirs

def run_mockparallel(mrs_prog, inputs, output, options):
    map_tasks = options.map_tasks
    reduce_tasks = options.reduce_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1

    if map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    from mrs.mapreduce import Operation
    op = Operation(mrs_prog, map_tasks=map_tasks, reduce_tasks=reduce_tasks)
    mrsjob = MockParallelJob(inputs, output, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


def run_serial(mrs_prog, inputs, output, options):
    """Mrs Serial
    """
    from mrs.mapreduce import Operation
    op = Operation(mrs_prog)
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
    def __init__(self, inputs, output, **kwds):
        Job.__init__(self, **kwds)
        self.inputs = inputs
        self.output = output
        self.debug = False

    def run(self):
        """Run a MapReduce operation in serial.
        """
        # TEMPORARY LIMITATIONS
        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        operation = self.operations[0]
        mrs_prog = operation.mrs_prog

        from itertools import chain
        # MAP PHASE
        input_files = [io.openfile(filename) for filename in self.inputs]
        all_input = chain(*input_files)
        map_output = mrs_map(mrs_prog.mapper, all_input)

        # SORT PHASE
        import operator
        interm = sorted(map_output, key=operator.itemgetter(0))

        # REDUCE PHASE
        output_file = operation.output_format(open(self.output, 'w'))
        for k, v in mrs_reduce(mrs_prog.reducer, interm):
            output_file.write(k, v)

        # cleanup
        for f in input_files:
            f.close()
        output_file.close()


class MockParallelJob(Job):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, inputs, outdir, shared_dir, **kwds):
        Job.__init__(self, **kwds)
        self.inputs = inputs
        self.outdir = outdir
        self.shared_dir = shared_dir

    def run(self):
        ################################################################
        # TEMPORARY LIMITATIONS
        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        op = self.operations[0]

        map_tasks = op.map_tasks
        if map_tasks != len(self.inputs):
            raise NotImplementedError("Requires exactly 1 map_task per input.")

        reduce_tasks = op.reduce_tasks
        ################################################################

        import sys, os
        from tempfile import mkstemp, mkdtemp

        # Prep:
        try_makedirs(self.outdir)
        try_makedirs(self.shared_dir)
        jobdir = mkdtemp(prefix='mrs.job_', dir=self.shared_dir)

        # Create Map Tasks:
        map_list = []
        for taskid, filename in enumerate(self.inputs):
            map_task = MapTask(taskid, op.mrs_prog, filename, jobdir,
                    reduce_tasks)
            map_list.append(map_task)

        # Create Reduce Tasks:
        reduce_list = []
        for taskid in xrange(op.reduce_tasks):
            reduce_task = ReduceTask(taskid, op.mrs_prog, self.outdir, jobdir)
            reduce_list.append(reduce_task)

        # Run Tasks:
        for task in map_list:
            task.run()
            outputs = task.output.filenames()
            for i, filename in enumerate(outputs):
                if filename is not None:
                    reduce_list[i].inputs.append(filename)

        for task in reduce_list:
            task.run()


# vim: et sw=4 sts=4

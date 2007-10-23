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

import io
from mapreduce import Job, mrs_reduce, MapTask, ReduceTask, interm_dir
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

        if len(self.inputs) != 1:
            raise NotImplementedError("Requires exactly one input file.")
        input = self.inputs[0]

        # MAP PHASE
        from itertools import starmap
        input_file = io.openfile(input)
        map_itr = starmap(mrs_prog.mapper, input_file)
        interm = [item for subitr in map_itr for item in subitr]
        input_file.close()

        # SORT PHASE
        import operator
        interm.sort(key=operator.itemgetter(0))

        # REDUCE PHASE
        output_file = operation.output_format(open(self.output, 'w'))
        mrs_reduce(mrs_prog.reducer, interm, output_file)
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
        for i in xrange(reduce_tasks):
            os.mkdir(interm_dir(jobdir, i))

        # Create Map Tasks:
        tasks = []
        for taskid, filename in enumerate(self.inputs):
            map_task = MapTask(taskid, op.mrs_prog, filename, jobdir,
                    reduce_tasks)
            tasks.append(map_task)

        # Create Reduce Tasks:
        for taskid in xrange(op.reduce_tasks):
            reduce_task = ReduceTask(taskid, op.mrs_prog, self.outdir, jobdir)
            tasks.append(reduce_task)

        for task in tasks:
            task.run()


# vim: et sw=4 sts=4

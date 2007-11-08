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
from mapreduce import Implementation, mrs_map, mrs_reduce, MapTask, ReduceTask
from util import try_makedirs

def run_mockparallel(registry, run, inputs, output, options):
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
    op = Operation(registry, run, map_tasks=map_tasks, reduce_tasks=reduce_tasks)
    mrsjob = MockParallel(inputs, output, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


def run_serial(registry, run, inputs, output, options):
    """Mrs Serial
    """
    from mrs.mapreduce import Operation
    op = Operation(registry, run)
    mrsjob = Serial(inputs, output)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


class Serial(Implementation):
    """MapReduce execution on a single processor
    """
    def __init__(self, inputs, output, **kwds):
        Implementation.__init__(self, **kwds)
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
        registry = operation.registry

        from itertools import chain
        # MAP PHASE
        input_files = [io.openfile(filename) for filename in self.inputs]
        all_input = chain(*input_files)
        map_output = mrs_map(registry['mapper'], all_input)

        # SORT PHASE
        import operator
        interm = sorted(map_output, key=operator.itemgetter(0))

        # REDUCE PHASE
        output_file = io.openfile(self.output, 'w')
        for k, v in mrs_reduce(registry['reducer'], interm):
            output_file.write(k, v)

        # cleanup
        for f in input_files:
            f.close()
        output_file.close()


class MockParallel(Implementation):
    """MapReduce execution on POSIX shared storage, such as NFS
    
    Specify a directory located in shared storage which can be used as scratch
    space.
    """
    def __init__(self, inputs, outdir, shared_dir, **kwds):
        Implementation.__init__(self, **kwds)
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
            map_task = MapTask(taskid, op.registry, jobdir, reduce_tasks)
            map_task.inputs = [filename]
            map_list.append(map_task)

        # Create Reduce Tasks:
        reduce_list = []
        for taskid in xrange(op.reduce_tasks):
            reduce_task = ReduceTask(taskid, op.registry, self.outdir)
            reduce_list.append(reduce_task)

        # Run Tasks:
        for task in map_list:
            task.run()
            outputs = task.output.filenames()
            for i, filename in enumerate(outputs):
                if filename:
                    reduce_list[i].inputs.append(filename)

        for task in reduce_list:
            task.run()


# vim: et sw=4 sts=4

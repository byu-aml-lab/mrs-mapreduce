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

import threading
from io import HexWriter, TextWriter

def mrs_simple(job, args, opts):
    """Default run function for a map phase and reduce phase"""
    if len(args) < 2:
        import sys
        print >>sys.stderr, "Requires input(s) and an output."
        sys.exit(-1)
    source = job.file_data(args[:-1])
    intermediate = job.map_data(source, 'mapper')
    output = job.reduce_data(intermediate, 'reducer', outdir=args[-1],
            format=TextWriter)


# TODO: add a DataSet for resplitting input.
# TODO: make everything reentrant once Parallel runs in a different thread.
class Job(threading.Thread):
    """Keep track of all operations that need to be performed.
    
    When run as a thread, call the user-specified run function, which will
    submit datasets to be computed.
    """
    def __init__(self, registry, jobdir, user_run, args, opts):
        threading.Thread.__init__(self)

        self.registry = registry
        self.jobdir = jobdir
        self.user_run = user_run
        self.args = args
        self.opts = opts

        self.default_reduce_parts = 1
        self.default_reduce_tasks = opts.mrs_reduce_tasks

        self._lock = threading.Lock()
        self.active_data = []
        self.waiting_data = []
        # Submitting more datasets is still allowed:
        self.submission_ok = True

    def run(self):
        """Run the job creation thread

        Call the user-specified run function, which will submit datasets to be
        computed.
        """
        job = self
        self.user_run(job, self.args, self.opts)
        self.submission_ok = False

    def submit(self, dataset):
        """Submit a DataSet to be computed.

        If it's ready to go, computation will begin promptly.  However, if
        it depends on other DataSets to complete, it will be added to a
        todo queue and will be run later.

        Called from the user-specified run function.
        """
        assert(self.submission_ok)
        self._lock.acquire()
        if dataset.ready():
            if not dataset.tasks_made:
                dataset.make_tasks()
            self.active_data.append(dataset)
        else:
            self.waiting_data.append(dataset)
        self._lock.release()

    def remove_dataset(self, dataset):
        """Remove a completed or waiting DataSet.

        Submit is usually called outside the Job thread.
        """

        if dataset in self.waiting_data:
            self.waiting_data.remove(dataset)
            return
        elif dataset in self.active_data:
            assert(dataset.done())
            return
        else:
            raise ValueError("DataSet not in job.")

    def check_done(self):
        """Check to see if any DataSets are done.
        """
        dataset_done = False
        self._lock.acquire()
        for dataset in self.active_data:
            if dataset.done():
                dataset_done = True
                self.active_data.remove(dataset)
        self._lock.release()

        if dataset_done:
            self.check_active()

    def check_active(self):
        """Activate any DataSets that are ready to be computed.

        A waiting DataSet may become ready whenever another DataSet completes.

        Activate_all is usually called outside the Job thread.
        """
        self._lock.acquire()
        for dataset in self.waiting_data:
            if dataset.ready():
                if not dataset.tasks_made:
                    dataset.make_tasks()
                self.waiting_data.remove(dataset)
                self.active_data.append(dataset)
        self._lock.release()

    def print_status(self):
        """Report on the status of all active tasks.

        Note that waiting DataSets are ignored.  This is necessary because
        a waiting DataSet might not have created its tasks yet.

        Print_status is usually called outside the Job thread.
        """
        if self.done():
            print 'Done'
        else:
            active = 0
            done = 0
            todo = 0

            self._lock.acquire()
            for dataset in self.active_data:
                active += len(dataset.tasks_active)
                done += len(dataset.tasks_done)
                todo += len(dataset.tasks_todo)
            self._lock.release()

            total = active + done + todo
            print 'Completed: %s/%s, Active: %s' % (done, total, active)

    def schedule(self):
        """Return the next task to be assigned.

        Schedule is usually called outside the Job thread.
        """
        if self.done():
            return None
        else:
            next_task = None

            self._lock.acquire()
            for dataset in self.active_data:
                next_task = dataset.get_task()
                if next_task is not None:
                    break
            self._lock.release()

            return next_task

    def done(self):
        """Report whether all computation is complete.

        Done is usually called outside the Job thread.
        """
        if self.submission_ok:
            return False
        elif self.active_data or self.waiting_data:
            return False
        else:
            return True

    def file_data(self, filenames):
        from datasets import FileData
        ds = FileData(filenames)
        return ds

    def map_data(self, input, mapper, nparts=None, outdir=None, parter=None,
            format=HexWriter):
        """Define a set of data computed with a map operation.

        Specify the input dataset and a mapper function.  The mapper must be
        in the job's registry and may be specified as a name or function.

        Called from the user-specified run function.
        """
        if nparts is None:
            nparts = self.default_reduce_tasks

        from datasets import MapData
        if outdir is None:
            outdir = self.jobdir
        ds = MapData(input, mapper, nparts, outdir, parter=parter,
                registry=self.registry, format=format)
        self.submit(ds)
        return ds

    def reduce_data(self, input, reducer, nparts=None, outdir=None,
            parter=None, format=HexWriter):
        """Define a set of data computed with a reducer operation.

        Specify the input dataset and a reducer function.  The reducer must be
        in the job's registry and may be specified as a name or function.

        Called from the user-specified run function.
        """
        if nparts is None:
            nparts = self.default_reduce_parts

        from datasets import ReduceData
        if outdir is None:
            outdir = self.jobdir
        ds = ReduceData(input, reducer, nparts, outdir, parter=parter,
                registry=self.registry, format=format)
        self.submit(ds)
        return ds


# vim: et sw=4 sts=4

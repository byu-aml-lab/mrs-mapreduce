# Mrs
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
from io import HexWriter


# TODO: add a DataSet for resplitting input.
class Job(threading.Thread):
    """Keep track of all operations that need to be performed.
    
    When run as a thread, call the user-specified run function, which will
    submit datasets to be computed.
    """
    def __init__(self, registry, user_run, user_setup, args, opts):
        threading.Thread.__init__(self)
        # Quit the whole program, even if this thread is still running:
        self.setDaemon(True)

        self.registry = registry
        self.user_run = user_run
        self.user_setup = user_setup
        self.args = args
        self.opts = opts

        self.default_reduce_parts = 1
        self.default_reduce_tasks = opts.mrs_reduce_tasks

        self._runwaitcv = threading.Condition()
        self._runwaitlist = None
        self._runwaitresult = []

        self._lock = threading.Lock()
        self.active_data = []
        self.waiting_data = []

        # Set up the job directory
        import tempfile
        shared_dir = self.opts.mrs_shared
        self.jobdir = tempfile.mkdtemp(prefix='mrs.job_', dir=shared_dir)

        # Still waiting for work to do:
        self._end = False
        self.update_callback = None
        self.end_callback = None

    def run(self):
        """Run the job creation thread

        Call the user-specified run function, which will submit datasets to be
        computed.
        """
        job = self
        die = False

        if self.user_setup:
            try:
                self.user_setup(self.opts)
            except Exception, e:
                # The user code threw some exception.  Print out the error.
                die = True

        if not die:
            try:
                self.user_run(job, self.args, self.opts)
            except Exception, e:
                die = True

        if die:
            # The user code threw some exception.  Print out the error.
            import traceback
            traceback.print_exc()
        self.end()

    def submit(self, dataset):
        """Submit a DataSet to be computed.

        If it's ready to go, computation will begin promptly.  However, if
        it depends on other DataSets to complete, it will be added to a
        todo queue and will be run later.

        Called from the user-specified run function.
        """
        assert(not self._end)
        self._lock.acquire()
        if dataset.ready():
            if not dataset.tasks_made:
                dataset.make_tasks()
            self.active_data.append(dataset)
        else:
            self.waiting_data.append(dataset)
        self._lock.release()

        if self.update_callback:
            self.update_callback()

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
            # See if the Job thread is ready to be awakened:
            self.wakeup()
            # See if there are any datasets ready to be activated:
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
        if self._end and not self.active_data and not self.waiting_data:
            return True
        else:
            return False

    def wakeup(self):
        """Wake up the Job thread if it is ready.
        
        The user-specified run function calls job.wait(*datasets) to wait.
        Wakeup is called from outside the Job thread and checks to see if the
        condition has been met.
        """
        self._runwaitcv.acquire()
        if self._runwaitlist:
            ready = [ds for ds in self._runwaitlist if ds.done()]

            if ready:
                self._runwaitlist = None
                self._runwaitresult = ready
                self._runwaitcv.notify()
        self._runwaitcv.release()

    def wait(self, *datasets, **kwds):
        """Wait for any of the given DataSets to complete.
        
        The optional timeout parameter specifies a floating point number
        of seconds to wait before giving up.  The wait function returns a
        list of datasets that are ready.
        """
        timeout = kwds.get('timeout', None)

        self._runwaitcv.acquire()
        self._runwaitlist = datasets
        self._runwaitresult = []

        self._runwaitcv.wait(timeout)

        ready = self._runwaitresult
        self._runwaitlist = None
        self._runwaitcv.release()
        return ready

    def status(self):
        """Report on the status of all active tasks.

        Returns a string.  Note that waiting DataSets are ignored.  This is
        necessary because a waiting DataSet might not have created its tasks
        yet.
        """
        if self.done():
            return 'Done'
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
            return 'Status: %s/%s done, %s active' % (done, total, active)

    def end(self):
        """Mark that all DataSets have already been submitted.

        After this point, any submit() will fail.
        """
        if not self._end:
            self._end = True
            if self.end_callback:
                self.end_callback()

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

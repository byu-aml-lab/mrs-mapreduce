# Mrs
# Copyright 2008-2011 Brigham Young University
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
# Inquiries regarding any further use of Mrs, please contact the Copyright
# Licensing Office, Brigham Young University, 3760 HBLL, Provo, UT 84602,
# (801) 422-9339 or 422-3821, e-mail copyright@byu.edu.

"""Mrs Runner"""

import collections
import multiprocessing
import os
from six.moves import xrange as range
import sys
import threading

from six import print_
from . import job
from . import peons
from . import computed_data
from . import util
from . import worker

import logging
logger = logging.getLogger('mrs')
del logging

INITIAL_PEON_THREADS = 4


class BaseRunner(object):
    """Communicates with the job thread and keeps track of datasets.

    BaseRunner is an abstract class that is used by both SerialRunner and
    TaskRunner.  The concepts in this class, such as communicating with
    the job process and managing the list of open datasets, are applicable
    to both types of runners.

    Arguments:
        program_class: class (inheriting from MapReduce) which defines
            methods such as run, map, reduce, partition, etc.
        opts: command-line options which are sent to workers
        args: command-line arguments which are sent to workers
        job_conn: connection (from the multiprocessing module) to/from the
            job process
        jobdir: optional shared directory for storage of output datasets
        default_dir: temporary directory for storage of output datasets

    Attributes:
        close_requests: set of Datasets requested to be closed by the job
            process (but which cannot be closed until their dependents are
            computed)
        computing_datasets: set of Datasets that are being or need to be
            computed
        data_dependents: maps a dataset id to a deque listing datasets that
            cannot start until it has finished
        datasets: maps a dataset id to the corresponding Dataset object
    """

    def __init__(self, program_class, opts, args, job_conn, jobdir,
            default_dir, worker_pipe):
        self.program_class = program_class
        self.opts = opts
        self.args = args
        self.job_conn = job_conn
        self.jobdir = jobdir
        self.default_dir = default_dir

        self.event_loop = util.EventLoop()
        self.event_loop.register_fd(self.job_conn.fileno(), self.read_job_conn)
        if worker_pipe is not None:
            self.worker_pipe = worker_pipe
            self.event_loop.register_fd(self.worker_pipe.fileno(),
                    self.read_worker_pipe)

        self.datasets = {}
        self.computing_datasets = set()
        self.data_dependents = collections.defaultdict(collections.deque)
        self.close_requests = set()

    def read_job_conn(self):
        try:
            message = self.job_conn.recv()
        except EOFError:
            return

        if isinstance(message, job.DatasetSubmission):
            ds = message.dataset
            self.datasets[ds.id] = ds
            input_id = getattr(ds, 'input_id', None)
            if input_id:
                self.data_dependents[input_id].append(ds.id)
            if isinstance(ds, computed_data.ComputedData):
                self.computing_datasets.add(ds)
                self.compute_dataset(ds)
        elif isinstance(message, job.CloseDataset):
            self.close_requests.add(message.dataset_id)
            self.try_to_close_dataset(message.dataset_id)
            self.try_to_remove_dataset(message.dataset_id)
        elif isinstance(message, job.JobDone):
            if message.exitcode != 0:
                logger.critical('Job execution failed.')
            self.exitcode = message.exitcode
            self.job_conn.send(job.QuitJobProcess())
            self.event_loop.running = False
        else:
            assert False, 'Unknown message type.'

    def run(self):
        raise NotImplementedError

    def compute_dataset(self, dataset):
        """Called when a new ComputedData set is submitted."""
        raise NotImplementedError

    def dataset_done(self, dataset):
        """Called when a dataset's computation is finished."""

        dataset.computation_done()
        self.computing_datasets.remove(dataset)

        # Check whether any datasets can be closed as a result of the newly
        # completed computation.
        if dataset.input_id:
            self.try_to_close_dataset(dataset.input_id)
        self.try_to_close_dataset(dataset.id)

        self.send_dataset_response(dataset)
        self.try_to_remove_dataset(dataset.id)

    def send_dataset_response(self, dataset):
        if not (dataset.closed or dataset.id in self.close_requests):
            for bucket in dataset[:, :]:
                if len(bucket) or bucket.url:
                    response = job.BucketReady(dataset.id, bucket)
                    self.job_conn.send(response)
        response = job.DatasetComputed(dataset.id, not dataset.closed)
        self.job_conn.send(response)

    def try_to_close_dataset(self, dataset_id):
        """Try to close the given dataset and remove its parent."""
        if dataset_id not in self.close_requests:
            return
        ds = self.datasets[dataset_id]
        # Skip datasets that are currently being computed.
        if ds in self.computing_datasets:
            return
        # Skip datasets that haven't been computed yet.
        if getattr(ds, 'computing', False):
            return
        # Bail out if any dependent dataset still needs to be computed.
        deplist = self.data_dependents[dataset_id]
        for dependent_id in deplist:
            dependent_ds = self.datasets[dependent_id]
            if getattr(dependent_ds, 'computing', False):
                return

        logger.debug('Closing dataset: %s' % dataset_id)
        ds.close()
        self.close_requests.remove(dataset_id)

        input_id = getattr(ds, 'input_id', None)
        if input_id:
            self.data_dependents[input_id].remove(dataset_id)
            self.try_to_remove_dataset(input_id)

    def try_to_remove_dataset(self, dataset_id):
        """Try to remove the given dataset.

        If a dataset is not closed or if any of its direct dependents are not
        closed, then it will not be removed.
        """
        ds = self.datasets.get(dataset_id, None)
        if ds is None:
            return
        if not ds.closed:
            return

        deplist = self.data_dependents[dataset_id]
        if not deplist:
            logger.debug('Removing dataset: %s' % dataset_id)
            self.remove_dataset(ds)
            del self.datasets[dataset_id]
            del self.data_dependents[dataset_id]

    def remove_dataset(self, dataset):
        if dataset.permanent:
            dataset.clear()
        else:
            dataset.delete()

    def debug_status(self):
        """Print out the debug info about the current status of the runner."""
        pass


class TaskRunner(BaseRunner):
    """Breaks down datasets into individual tasks.

    By default, the tasks are evaluated in serial, but the schedule method
    may be overridden.

    Attributes:
        pending_datasets: set of datasets that are not yet runnable
        runnable_datasets: deque of datasets that are ready to run but have
            not yet been split into tasks
        ready_tasks: list of (dataset, source) pairs
        tasklists: map from a dataset id to the corresponding TaskList
    """

    def __init__(self, *args):
        super(TaskRunner, self).__init__(*args)

        self.pending_datasets = set()
        self.runnable_datasets = collections.deque()
        self.tasklists = {}
        self.forward_links = collections.defaultdict(set)

        self.chore_queue_pipe, chore_queue_write_pipe = os.pipe()
        self.event_loop.register_fd(self.chore_queue_pipe,
                self.read_chore_queue_pipe)
        self.chore_queue = peons.ChoreQueue(chore_queue_write_pipe)
        self.peon_thread_count = 0

    def start_peon_thread(self):
        """Starts a PeonThreads.

        Note that this method should only be called after all forking has
        completed.  Threads and forking do not play well together.
        """
        peons.start_peon_thread(self.chore_queue)
        self.peon_thread_count += 1

    def read_chore_queue_pipe(self):
        """Reads currently available data from chore_queue_pipe.

        The actual data is ignored--the pipe is just a mechanism for
        interrupting the select loop if it's blocking.
        """
        os.read(self.chore_queue_pipe, 4096)

    def compute_dataset(self, dataset):
        if self._runnable_or_pending(dataset):
            self.schedule()

    def next_task(self):
        """Returns the next available task, or None if none are available.

        Tasks are returned as (dataset, source) pairs.
        """
        for ds in self.runnable_datasets:
            try:
                tasklist = self.tasklists[ds.id]
            except KeyError:
                tasklist = self.make_tasklist(ds)
            t = tasklist.pop()
            if t is not None:
                return t
        return None

    def make_tasklist(self, ds):
        """Makes a tasklist for the given dataset."""
        logger.info('Making tasks for dataset: %s' % ds.id)
        assert ds.computing, "can't make tasks for a completed dataset"
        tasklist = TaskList(ds, self.datasets[ds.input_id])
        self.tasklists[ds.id] = tasklist
        tasklist.make_tasks()
        return tasklist

    def task_done(self, dataset_id, task_index, urls):
        """Report that the given source of the given dataset is computed.

        Arguments:
            dataset_id: string
            task_index: integer id of the task that produced the data
            urls: list of (number, string) pairs representing the split and
                url of the outputs.
        """
        tasklist = self.tasklists[dataset_id]
        tasklist.task_done(task_index)

        dataset = self.datasets[dataset_id]
        if not dataset.closed:
            for split, url in urls:
                bucket = dataset[task_index, split]
                bucket.url = url
                if dataset_id not in self.close_requests:
                    response = job.BucketReady(dataset_id, bucket)
                    self.job_conn.send(response)
            dataset.notify_urls_known()

        if tasklist.complete():
            self.dataset_done(dataset)

        self._wakeup_dependents(dataset_id)

    def task_lost(self, dataset_id, task_index):
        """Report that a task was lost (e.g., to a dead slave)."""
        tasklist = self.tasklists.get(dataset_id)
        if tasklist is not None:
            tasklist.push(task_index)

    def dataset_done(self, dataset):
        del self.tasklists[dataset.id]
        self.runnable_datasets.remove(dataset)
        super(TaskRunner, self).dataset_done(dataset)

    def _wakeup_dependents(self, dataset_id):
        """Move any dependent datasets possible from pending to runnable."""

        try:
            ds = self.datasets[dataset_id]
        except KeyError:
            return
        if ds.computing:
            percent_complete = self.tasklists[dataset_id].percent_complete()
        else:
            percent_complete = 1

        if percent_complete < 1:
            return

        for dependent_id in self.data_dependents[dataset_id]:
            dep_ds = self.datasets[dependent_id]
            if (dep_ds in self.pending_datasets and
                    percent_complete == 1):
                self.pending_datasets.remove(dep_ds)
                self.runnable_datasets.append(dep_ds)

    def send_dataset_response(self, dataset):
        response = job.DatasetComputed(dataset.id, False)
        self.job_conn.send(response)

    def _runnable_or_pending(self, ds):
        """Add the dataset to runnable or pending list as appropriate.

        Returns whether the dataset is runnable.
        """
        input_ds = self.datasets.get(ds.input_id, None)
        if (input_ds is None) or getattr(input_ds, 'computing', False):
            self.pending_datasets.add(ds)
            return False
        else:
            self.runnable_datasets.append(ds)
            return True

    def schedule(self):
        raise NotImplementedError

    def remove_dataset(self, dataset):
        if dataset.permanent:
            dataset.clear()
        else:
            self.chore_queue.do(dataset.delete)

    def debug_status(self):
        super(TaskRunner, self).debug_status()
        print_('Runnable datasets:', (', '.join(ds.id
                for ds in self.runnable_datasets)), file=sys.stderr)
        print_('Pending datasets:', (', '.join(ds.id
                for ds in self.pending_datasets)), file=sys.stderr)
        print_('Ready tasks:')
        for ds in self.runnable_datasets:
            if ds in self.tasklists:
                sources = (str(t[1]) for t in self.tasklists[ds])
                print_('    %s:' % ds_id, ', '.join(sources))


class TaskList(object):
    """Manages the list of tasks associated with a single dataset."""

    def __init__(self, dataset, input_ds):
        self.dataset = dataset
        self.input_ds = input_ds
        self._remaining_tasks = set()
        self._ready_tasks = collections.deque()
        self._tasks_made = False

    def make_tasks(self):
        """Generate tasks for the given dataset, adding them to ready_tasks.

        Note that a task is not created if the corresponding split in the
        input dataset is empty.
        """
        for task_index in range(self.dataset.ntasks):
            for b in self.input_ds[:, task_index]:
                if b.url:
                    self._ready_tasks.append(task_index)
                    self._remaining_tasks.add(task_index)
                    break
        self._tasks_made = True

    def percent_complete(self):
        """Returns the percent of datasets that have been computed."""
        if self._tasks_made:
            return len(self._remaining_tasks) / self.dataset.ntasks
        else:
            return 0

    def complete(self):
        return not self._remaining_tasks

    def task_done(self, task_index):
        self._remaining_tasks.remove(task_index)

    def pop(self):
        """Pop off the next available task (or None if none are available).

        Returns a (dataset_id, task_index) pair.
        """
        assert self._tasks_made
        try:
            return (self.dataset.id, self._ready_tasks.popleft())
        except IndexError:
            return None

    def push(self, task_index):
        """Push back a task.

        Called if, for example, a Task is aborted.
        """
        self._ready_tasks.appendleft(task_index)

    def __iter__(self):
        """Iterate over tasks ((dataset_id, task_index) pairs)."""
        for task_index in self._ready_tasks:
            yield (self.dataset.id, task_index)


class MockParallelRunner(TaskRunner, worker.WorkerManager):
    def __init__(self, *args):
        super(MockParallelRunner, self).__init__(*args)

        self.program = None
        self.current_task = None

    def run(self):
        for i in xrange(INITIAL_PEON_THREADS):
            self.start_peon_thread()
        self.worker_setup(self.opts, self.args, self.default_dir)
        self.schedule()
        self.event_loop.run()
        return self.exitcode

    def schedule(self):
        assert self.current_task is None
        next_task = self.next_task()
        if next_task is not None:
            dataset_id, task_index = next_task
            ds = self.datasets[dataset_id]
            task = ds.get_task(task_index, self.datasets, self.jobdir)
            request = worker.WorkerTaskRequest(*task.to_args())
            result = self.submit_request(request)
            assert result

    def worker_success(self, r):
        """Called when a worker sends a WorkerSuccess."""
        self.task_done(r.dataset_id, r.task_index, r.outurls)
        self.schedule()

    def worker_failure(self, r):
        """Called when a worker sends a WorkerSuccess."""
        raise RuntimeError('Task failed')

# vim: et sw=4 sts=4

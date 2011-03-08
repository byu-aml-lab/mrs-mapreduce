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
import select

from . import datasets
from . import job

import logging
logger = logging.getLogger('mrs')


class BaseRunner(object):
    def __init__(self, program_class, opts, args, job_conn):
        self.program_class = program_class
        self.opts = opts
        self.args = args
        self.job_conn = job_conn

        #self.running = True

        self.datasets = {}
        self.data_dependents = collections.defaultdict(set)
        # Datasets requested to be closed by the job process (but which
        # cannot be closed until their dependents are computed).
        self.close_requests = set()

    """
    def run(self):
        poll = select.poll()
        poll.register(self.job_conn, select.POLLIN)
        poll.register(self.worker_conn, select.POLLIN)

        while self.running:
            for fd, event in poll.poll():
                if fd == self.job_conn.fileno():
                    self.read_job_conn()
                elif fd == self.worker_conn.fileno():
                    self.read_worker_conn()
                else:
                    assert False
    """

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
                self.data_dependents[input_id].add(ds.id)
            if isinstance(ds, datasets.ComputedData):
                self.compute_dataset(ds)
        elif isinstance(message, job.CloseDataset):
            self.close_requests.add(message.dataset_id)
            self.try_to_close_dataset(message.dataset_id)
            self.try_to_remove_dataset(message.dataset_id)
        elif isinstance(message, job.JobDone):
            if not message.success:
                logger.critical('Job execution failed.')
            self.job_conn.send(job.JobDoneAck())
            self.running = False
        else:
            assert False, 'Unknown message type.'

    def compute_dataset(self, dataset):
        """Called when a new ComputedData set is submitted."""
        raise NotImplementedError

    def dataset_computed(self, dataset):
        """Called when a dataset's computation is finished."""

        # Check whether any datasets can be closed as a result of the newly
        # completed computation.
        if ds.input_id:
            self.try_to_close_dataset(ds.input_id)
        self.try_to_close_dataset(dataset_id)

        if not ds.closed:
            for bucket in ds:
                if len(bucket) or bucket.url:
                    response = job.BucketReady(ds.id, bucket)
                    self.job_conn.send(response)
        response = job.DatasetComputed(ds.id, not ds.closed)
        self.job_conn.send(response)
        self.try_to_remove_dataset(ds.id)

    def try_to_close_dataset(self, dataset_id):
        """Try to close the given dataset and remove its parent."""
        if dataset_id not in self.close_requests:
            return
        # Bail out if any dependent dataset still needs to be computed.
        depset = self.data_dependents[dataset_id]
        for dependent_id in depset:
            dependent_ds = self.datasets[dependent_id]
            if not getattr(dependent_ds, 'computed', True):
                return

        ds = self.datasets[dataset_id]
        ds.close()
        self.close_requests.remove(dataset_id)

        input_id = getattr(ds, 'input_id', None)
        if input_id:
            self.data_dependents[input_id].discard(dataset_id)
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

        depset = self.data_dependents[dataset_id]
        if not depset:
            del self.datasets[dataset_id]
            del self.data_dependents[dataset_id]


# vim: et sw=4 sts=4

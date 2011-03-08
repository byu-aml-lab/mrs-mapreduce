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

"""Mrs Serial Runner"""

import multiprocessing
import select
import threading

from . import runner

import logging
logger = logging.getLogger('mrs')


class SerialRunner(runner.BaseRunner):
    def __init__(self, program_class, opts, args, job_conn):
        super(SerialRunner, self).__init__(program_class, opts, args, job_conn)

        self.running = True
        self.program = None
        self.worker_conn = None

    def run(self):
        try:
            self.program = self.program_class(self.opts, self.args)
        except Exception, e:
            import traceback
            logger.critical('Exception while instantiating the program: %s'
                    % traceback.format_exc())
            return

        self.start_worker()

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

    def start_worker(self):
        self.worker_conn, remote_worker_conn = multiprocessing.Pipe()
        worker = SerialWorker(self.program, self.datasets, remote_worker_conn)
        worker_thread = threading.Thread(target=worker.run,
                name='Serial Worker')
        worker_thread.daemon = True
        worker_thread.start()

    def compute_dataset(self, dataset):
        """Called when a new ComputedData set is submitted."""
        self.worker_conn.send(dataset.id)

    def read_worker_conn(self):
        """Read a message from the worker.

        Each message is the id of a dataset that has finished being computed.
        """
        try:
            dataset_id = self.worker_conn.recv()
        except EOFError:
            return
        ds = self.datasets[dataset_id]
        self.dataset_computed(ds)


class SerialWorker(object):
    def __init__(self, program, datasets, conn):
        self.program = program
        self.datasets = datasets
        self.conn = conn

    def run(self):
        while True:
            try:
                dataset_id = self.conn.recv()
            except EOFError:
                return
            ds = self.datasets[dataset_id]
            ds.run_serial(self.program, self.datasets)
            self.conn.send(dataset_id)

# vim: et sw=4 sts=4

# Mrs
# Copyright 2008-2012 Brigham Young University
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

"""Mrs Worker

The worker process executes the user's map function and reduce function.
That's it.  It just does what the main slave process tells it to.  The worker
process is terminated when the main process quits.
"""

import os
import traceback

from . import datasets
from . import tasks
from . import util

from logging import getLogger
logger = getLogger('mrs')


class WorkerSetupRequest(object):
    """Request the worker to run the setup function."""

    def __init__(self, opts, args, default_dir):
        self.id = 'worker_setup'
        self.opts = opts
        self.args = args
        self.default_dir = default_dir

    def id(self):
        return self.__class__.__name__


class WorkerRemoveRequest(object):
    def __init__(self, *args):
        (self.directory,) = args

    def id(self):
        return self.__class__.__name__


class WorkerTaskRequest(object):
    """Request the to worker to run a task."""

    def __init__(self, *args):
        _, _, self.dataset_id, self.task_index, _, _, _, _, _ = args
        self.args = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.task_index)


class WorkerQuitRequest(object):
    """Request the worker to quit."""


class WorkerFailure(object):
    """Failure response from worker."""
    def __init__(self, dataset_id, task_index, exception, traceback,
            request_id):
        self.dataset_id = dataset_id
        self.task_index = task_index
        self.exception = exception
        self.traceback = traceback
        self.request_id = request_id


class WorkerSetupSuccess(object):
    """Successful worker setup."""


class WorkerSuccess(object):
    """Successful response from worker."""
    def __init__(self, dataset_id, task_index, outdir, outurls, request_id):
        self.dataset_id = dataset_id
        self.task_index = task_index
        self.outdir = outdir
        self.outurls = outurls
        self.request_id = request_id


class Worker(object):
    """Execute map tasks and reduce tasks.

    The worker waits for other threads to make assignments by calling
    start_map and start_reduce.

    This needs to run in a daemon thread rather than in the main thread so
    that it can be killed by other threads.
    """
    def __init__(self, program_class, request_pipe):
        self.program_class = program_class
        self.request_pipe = request_pipe
        self.default_dir = None
        self.program = None

    def run(self):
        while self.run_once():
            pass

    def run_once(self):
        """Runs one iteration of the event loop.

        Returns True if it should keep running.
        """
        request = None
        response = None

        try:
            request = self.request_pipe.recv()

            if isinstance(request, WorkerSetupRequest):
                assert self.program is None
                opts = request.opts
                args = request.args
                logger.debug('Starting to run the user setup function.')
                self.program = self.program_class(opts, args)
                self.default_dir = request.default_dir
                response = WorkerSetupSuccess()

            elif isinstance(request, WorkerQuitRequest):
                return False

            elif isinstance(request, WorkerRemoveRequest):
                util.remove_recursive(request.directory)

            else:
                assert self.program is not None
                logger.info('Running task: %s, %s' %
                        (request.dataset_id, request.task_index))
                t = tasks.Task.from_args(*request.args, program=self.program)
                t.run(self.program, self.default_dir)
                response = WorkerSuccess(request.dataset_id,
                        request.task_index, t.outdir, t.outurls(),
                        request.id())
                logger.info('Completed task: %s, %s' %
                        (request.dataset_id, request.task_index))
        except KeyboardInterrupt:
            return
        except Exception as e:
            logger.info('Failed task: %s, %s' %
                    (request.dataset_id, request.task_index))
            request_id = request.id() if request else None
            tb = traceback.format_exc()
            response = WorkerFailure(request.dataset_id, request.task_index,
                    e, tb, request_id)

        if response:
            self.request_pipe.send(response)

        return True

    def profiled_run(self):
        #TODO: detect the node number for other systems (e.g., pbs)
        nodenum = os.getenv('PSSH_NODENUM')
        if nodenum:
            filename = 'mrs-worker-%s.prof' % nodenum
        else:
            filename = 'mrs-worker.prof'
        util.profile_loop(self.run_once, (), {}, filename)


class WorkerManager(object):
    """Mixin class that provides methods for dealing with Workers.

    Assumes that a worker_pipe attribute is defined and that read_worker_pipe
    is called when data is available.  Also assumes that a current_task
    attribute is available.
    """
    def worker_setup(self, opts, args, default_dir):
        request = WorkerSetupRequest(opts, args, default_dir)
        self.worker_pipe.send(request)
        response = self.worker_pipe.recv()
        if isinstance(response, WorkerSetupSuccess):
            return True
        if isinstance(response, WorkerFailure):
            msg = 'Exception in Worker Setup: %s' % response.exception
            logger.critical(msg)
            msg = 'Traceback: %s' % response.traceback
            logger.error(msg)
            return False
        else:
            raise RuntimeError('Invalid message type.')

    def read_worker_pipe(self):
        """Reads a single response from the worker pipe."""

        r = self.worker_pipe.recv()
        if not (isinstance(r, WorkerSuccess) or isinstance(r, WorkerFailure)):
            assert False, 'Unexpected response type'

        assert self.current_task == (r.dataset_id, r.task_index)
        self.current_task = None

        if isinstance(r, WorkerSuccess):
            self.worker_success(r)
        elif isinstance(r, WorkerFailure):
            msg = 'Exception in Worker: %s' % r.exception
            logger.critical(msg)
            msg = 'Traceback: %s' % r.traceback
            logger.error(msg)

            self.worker_failure(r)

    def submit_request(self, request):
        """Submit the given request to the worker.

        If one_at_a_time is specified, then no other one_at_time requests can
        be accepted until the current task finishes.  Returns a boolean
        indicating whether the request was accepted.

        Called from the RPC thread.
        """
        if isinstance(request, WorkerTaskRequest):
            if self.current_task is not None:
                return False
            self.current_task = (request.dataset_id, request.task_index)

        self.worker_pipe.send(request)
        return True

    def worker_success(self, response):
        """Called when a worker sends a WorkerSuccess for the given task."""
        raise NotImplementedError

    def worker_failure(self, response):
        """Called when a worker sends a WorkerFailure for the given task."""
        raise NotImplementedError

# vim: et sw=4 sts=4

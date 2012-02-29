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

"""Mrs Worker

The worker process executes the user's map function and reduce function.
That's it.  It just does what the main slave process tells it to.  The worker
process is terminated when the main process quits.
"""

import traceback

from . import datasets
from . import fileformats
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


class WorkerMapRequest(object):
    """Request the worker to run a map task."""

    def __init__(self, *args):
        (self.dataset_id, self.task_index, self.inputs, self.map_name,
                self.part_name, self.splits, self.outdir, self.extension) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.task_index)

    def make_task(self, default_dir):
        input_data = datasets.FileData(self.inputs, splits=1,
                first_split=self.task_index)
        if self.extension:
            format = fileformats.writerformat(self.extension)
        else:
            format = fileformats.default_write_format

        if self.outdir:
            permanent = True
        else:
            self.outdir = util.mktempdir(default_dir, self.dataset_id + '_')
            permanent = False

        op = tasks.MapOperation(map_name=self.map_name,
                part_name=self.part_name)
        t = tasks.MapTask(op, input_data, self.task_index, self.splits,
                self.outdir, format, permanent)
        return t


class WorkerReduceRequest(object):
    """Request the to worker to run a reduce task."""

    def __init__(self, *args):
        (self.dataset_id, self.task_index, self.inputs, self.reduce_name,
                self.part_name, self.splits, self.outdir,
                self.extension) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.task_index)

    def make_task(self, default_dir):
        """Tell this worker to start working on a reduce task.

        This will ordinarily be called from some other thread.
        """
        input_data = datasets.FileData(self.inputs, splits=1,
                first_split=self.task_index)
        if self.extension:
            format = fileformats.writerformat(self.extension)
        else:
            format = fileformats.default_write_format

        if self.outdir:
            permanent = True
        else:
            self.outdir = util.mktempdir(default_dir, self.dataset_id + '_')
            permanent = False

        op = tasks.ReduceOperation(reduce_name=self.reduce_name,
                part_name=self.part_name)
        t = tasks.ReduceTask(op, input_data, self.task_index, self.splits,
                self.outdir, format, permanent)
        return t


class WorkerReduceMapRequest(object):
    """Request the to worker to run a reducemap task."""

    def __init__(self, *args):
        (self.dataset_id, self.task_index, self.inputs, self.reduce_name,
                self.map_name, self.part_name, self.splits, self.outdir,
                self.extension) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.task_index)

    def make_task(self, default_dir):
        """Tell this worker to start working on a reducemap task.

        This will ordinarily be called from some other thread.
        """
        input_data = datasets.FileData(self.inputs, splits=1,
                first_split=self.task_index)
        if self.extension:
            format = io.writerformat(self.extension)
        else:
            format = io.default_write_format

        if self.outdir:
            permanent = True
        else:
            self.outdir = util.mktempdir(default_dir, self.dataset_id + '_')
            permanent = False

        op = tasks.ReduceMapOperation(reduce_name=self.reduce_name,
                map_name=self.map_name, part_name=self.part_name)
        t = tasks.ReduceMapTask(op, input_data, 0, self.task_index,
                self.splits, self.outdir, format, permanent)
        return t


class WorkerQuitRequest(object):
    """Request the worker to quit."""


class WorkerFailure(object):
    """Failure response from worker."""
    def __init__(self, exception, traceback, request_id):
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
                logger.info('Starting to run a new task.')
                t = request.make_task(self.default_dir)
                t.run(self.program)
                response = WorkerSuccess(request.dataset_id,
                        request.task_index, request.outdir, t.outurls(),
                        request.id())
                logger.debug('Task complete.')
        except KeyboardInterrupt:
            return
        except Exception as e:
            request_id = request.id() if request else None
            tb = traceback.format_exc()
            response = WorkerFailure(e, tb, request_id)

        if response:
            self.request_pipe.send(response)

        return True

    def profiled_run(self):
        util.profile_loop(self.run_once, (), {}, 'mrs-worker.prof')


# vim: et sw=4 sts=4

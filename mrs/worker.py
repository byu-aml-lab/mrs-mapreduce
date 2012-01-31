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

import tempfile
import traceback

from . import datasets
from . import fileformats
from . import task
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
        (self.dataset_id, self.source, self.inputs, self.map_name,
                self.part_name, self.splits, self.outdir, self.extension
                ) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.source)

    def make_task(self, program, default_dir):
        input_data = datasets.FileData(self.inputs, splits=1)
        if self.extension:
            format = fileformats.writerformat(self.extension)
        else:
            format = fileformats.default_write_format

        if not self.outdir:
            self.outdir = tempfile.mkdtemp(dir=default_dir,
                    prefix=(self.dataset_id + '_'))

        mapper = getattr(program, self.map_name)
        parter = getattr(program, self.part_name)

        t = task.MapTask(input_data, 0, self.source, mapper, parter,
                self.splits, self.outdir, format)
        return t


class WorkerReduceRequest(object):
    """Request the to worker to run a reduce task."""

    def __init__(self, *args):
        (self.dataset_id, self.source, self.inputs, self.reduce_name,
                self.part_name, self.splits, self.outdir, self.extension
                ) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.source)

    def make_task(self, program, default_dir):
        """Tell this worker to start working on a reduce task.

        This will ordinarily be called from some other thread.
        """
        input_data = datasets.FileData(self.inputs, splits=1)
        if self.extension:
            format = fileformats.writerformat(self.extension)
        else:
            format = fileformats.default_write_format

        if not self.outdir:
            self.outdir = tempfile.mkdtemp(dir=default_dir,
                    prefix=(self.dataset_id + '_'))

        reducer = getattr(program, self.reduce_name)
        parter = getattr(program, self.part_name)

        t = task.ReduceTask(input_data, 0, self.source, reducer, parter,
                self.splits, self.outdir, format)
        return t


class WorkerReduceMapRequest(object):
    """Request the to worker to run a reducemap task."""

    def __init__(self, *args):
        (self.dataset_id, self.source, self.inputs, self.reducemap_name,
                self.part_name, self.splits, self.outdir, self.extension
                ) = args

    def id(self):
        return '%s_%s_%s' % (self.__class__.__name__, self.dataset_id,
                self.source)

    def make_task(self, program, default_dir):
        """Tell this worker to start working on a reducemap task.

        This will ordinarily be called from some other thread.
        """
        input_data = datasets.FileData(self.inputs, splits=1)
        if self.extension:
            format = io.writerformat(self.extension)
        else:
            format = io.default_write_format

        if not self.outdir:
            self.outdir = tempfile.mkdtemp(dir=default_dir,
                    prefix=(self.dataset_id + '_'))

        reducemapr = getattr(program, self.reducemap_name)
        parter = getattr(program, self.part_name)

        t = task.ReduceMapTask(input_data, 0, self.source, reducemapr, parter,
                self.splits, self.outdir, format)
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
    def __init__(self, dataset_id, source, outdir, outurls, request_id):
        self.dataset_id = dataset_id
        self.source = source
        self.outdir = outdir
        self.outurls = outurls
        self.request_id = request_id


def run_worker(program_class, request_pipe):
    """Execute map tasks and reduce tasks.

    The worker waits for other threads to make assignments by calling
    start_map and start_reduce.

    This needs to run in a daemon thread rather than in the main thread so
    that it can be killed by other threads.
    """
    default_dir = None
    program = None

    while True:
        request = None
        response = None

        try:
            request = request_pipe.recv()

            if isinstance(request, WorkerSetupRequest):
                assert program is None
                opts = request.opts
                args = request.args
                logger.debug('Starting to run the user setup function.')
                program = program_class(opts, args)
                default_dir = request.default_dir
                response = WorkerSetupSuccess()

            elif isinstance(request, WorkerQuitRequest):
                return

            elif isinstance(request, WorkerRemoveRequest):
                util.remove_recursive(request.directory)

            else:
                assert program is not None
                logger.info('Starting to run a new task.')
                t = request.make_task(program, default_dir)
                t.run()
                response = WorkerSuccess(request.dataset_id, request.source,
                        request.outdir, t.outurls(), request.id())
                logger.debug('Task complete.')
        except KeyboardInterrupt:
            return
        except Exception, e:
            request_id = request.id() if request else None
            tb = traceback.format_exc()
            response = WorkerFailure(e, tb, request_id)

        if response:
            request_pipe.send(response)


# vim: et sw=4 sts=4

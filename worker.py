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

from logging import getLogger
logger = getLogger('mrs')


class WorkerSetupRequest(object):
    """Request to worker to run setup function."""

    def __init__(self, opts, args, default_dir):
        self.opts = opts
        self.args = args
        self.default_dir = default_dir


class WorkerMapRequest(object):
    """Request to worker to run a map task."""

    def __init__(self, *args):
        (self.source, self.inputs, self.map_name, self.part_name, self.splits,
                self.outdir, self.extension) = args

    def make_task(self, program, default_dir):
        from task import MapTask
        from datasets import FileData
        from io.load import writerformat
        import tempfile

        input_data = FileData(self.inputs, splits=1)
        format = writerformat(self.extension)

        if not self.outdir:
            self.outdir = tempfile.mkdtemp(dir=default_dir, prefix='map_')

        mapper = getattr(program, self.map_name)
        parter = getattr(program, self.part_name)
        task = MapTask(input_data, 0, self.source, mapper, parter,
                self.splits, self.outdir, format)
        return task


class WorkerReduceRequest(object):
    """Request to worker to run a reduce task."""

    def __init__(self, *args):
        (self.source, self.inputs, self.reduce_name, self.part_name,
                self.splits, self.outdir, self.extension) = args

    def make_task(self, program, default_dir):
        """Tell this worker to start working on a reduce task.

        This will ordinarily be called from some other thread.
        """
        from task import ReduceTask
        from datasets import FileData
        from io.load import writerformat
        import tempfile

        input_data = FileData(self.inputs, splits=1)
        format = writerformat(self.extension)

        if not self.outdir:
            self.outdir = tempfile.mkdtemp(dir=default_dir, prefix='reduce_')

        reducer = getattr(program, self.reduce_name)
        parter = getattr(program, self.part_name)
        task = ReduceTask(input_data, 0, self.source, reducer,
                parter, self.splits, self.outdir, format)
        return task


class WorkerFailure(object):
    """Failure response from worker."""
    def __init__(self, exception, traceback):
        self.exception = exception
        self.traceback = traceback


class WorkerSuccess(object):
    """Successful response from worker."""
    def __init__(self, outurls=None):
        self.outurls = outurls


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
        request = request_pipe.recv()
        try:
            if isinstance(request, WorkerSetupRequest):
                assert program is None
                opts = request.opts
                args = request.args
                logger.debug('Starting to run the user setup function.')
                program = program_class(opts, args)
                default_dir = request.default_dir
                response = WorkerSuccess()
            else:
                assert program is not None
                logger.info('Starting to run a new task.')
                task = request.make_task(program, default_dir)
                task.run()
                response = WorkerSuccess(task.outurls())
                logger.debug('Task complete.')
        except Exception, e:
            import traceback
            tb = traceback.format_exc()
            response = WorkerFailure(e, tb)
        request_pipe.send(response)


# vim: et sw=4 sts=4

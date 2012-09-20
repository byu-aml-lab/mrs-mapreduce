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

from __future__ import division

import multiprocessing
import os
import select
import threading
import time
import traceback
import weakref

from . import bucket
from . import computed_data
from . import datasets
from . import http
from . import registry
from .serializers import Serializers
from . import tasks
from . import util

from logging import getLogger
logger = getLogger('mrs')


class Job(object):
    """Keep track of all operations that need to be performed.

    When run as a thread, call the user-specified run function, which will
    submit datasets to be computed.
    """
    def __init__(self, manager, program, opts, default_dir=None,
            url_converter=None):
        self._manager = manager
        self._program = program
        self._default_dir = default_dir
        self._url_converter = url_converter

        self._registry = registry.Registry(program)
        self._keep_jobdir = getattr(opts, 'mrs__keep_jobdir', False)
        self.default_partition = program.partition
        self.default_reduce_tasks = getattr(opts, 'mrs__reduce_tasks', 1)
        self.default_reduce_splits = 1

    def wait(self, *datasets, **kwds):
        """Wait for any of the given Datasets to complete.

        The optional timeout parameter specifies a floating point number
        of seconds to wait before giving up.  The wait function returns a
        list of datasets that are ready.
        """
        return self._manager.wait(*datasets, **kwds)

    def file_data(self, filenames):
        """Defines a set of data from a list of urls."""
        ds = datasets.FileData(filenames)
        self._manager.submit(ds)
        ds._close_callback = self._manager.close_dataset
        return ds

    def local_data(self, itr, splits=None, outdir=None, parter=None, **kwds):
        """Defines a set of data to be built locally from a given iterator.

        If the `parter` function is specified, then it is used to partition
        data by keys.  The `splits` parameter defines the total number of
        buckets.  If the `parter` is unspecified, then data are assigned to
        buckets on a round-robin basis, and if `splits` is also unspecified,
        then the number of buckets will grow with the size of the iterator.
        If the `parter` is specified, then the `splits` parameter is required.
        """
        if parter is not None and splits is None:
            raise RuntimeError('The splits parameter is required.')

        permanent = True
        if not outdir:
            if self._default_dir:
                outdir = util.mktempdir(self._default_dir, 'output_')
                permanent = self._keep_jobdir
        if outdir:
            util.try_makedirs(outdir)

        ds = datasets.LocalData(itr, splits, dir=outdir, parter=parter,
                permanent=permanent, **kwds)
        if self._url_converter:
            for bucket in ds[:, :]:
                bucket.url = self._url_converter.local_to_global(bucket.url)
        self._manager.submit(ds)
        ds._close_callback = self._manager.close_dataset
        return ds

    def map_data(self, input, mapper, splits=None, outdir=None, combiner=None,
            parter=None, **kwds):
        """Define a set of data computed with a map operation.

        Specify the input dataset and a mapper function.  The mapper must be
        in the program instance.

        Called from the user-specified run function.
        """
        if splits is None:
            splits = self.default_reduce_tasks
        assert isinstance(splits, int)

        if outdir:
            permanent = True
            util.try_makedirs(outdir)
        else:
            permanent = False

        if not parter:
            parter = self.default_partition

        map_name = self._registry[mapper]
        self._set_serializers(mapper, kwds)
        if combiner:
            combine_name = self._registry[combiner]
        else:
            combine_name = ''
        part_name = self._registry[parter]

        op = tasks.MapOperation(map_name, combine_name, part_name)
        ds = computed_data.ComputedData(op, input, splits=splits, dir=outdir,
                permanent=permanent, **kwds)
        self._manager.submit(ds)
        ds._close_callback = self._manager.close_dataset
        return ds

    def reduce_data(self, input, reducer, splits=None, outdir=None,
            parter=None, **kwds):
        """Define a set of data computed with a reducer operation.

        Specify the input dataset and a reducer function.  The reducer must be
        in the program instance.

        Called from the user-specified run function.
        """
        if splits is None:
            splits = self.default_reduce_splits

        if outdir:
            permanent = True
            util.try_makedirs(outdir)
        else:
            permanent = False

        if not parter:
            parter = self.default_partition

        reduce_name = self._registry[reducer]
        self._set_serializers(reducer, kwds)
        part_name = self._registry[parter]

        op = tasks.ReduceOperation(reduce_name, part_name)
        ds = computed_data.ComputedData(op, input, splits=splits, dir=outdir,
                permanent=permanent, **kwds)
        self._manager.submit(ds)
        ds._close_callback = self._manager.close_dataset
        return ds

    def reducemap_data(self, input, reducer, mapper, splits=None, outdir=None,
            combiner=None, parter=None, **kwds):
        """Define a set of data computed with the reducemap operation.

        Called from the user-specified run function.
        """
        if splits is None:
            splits = self.default_reduce_tasks

        if outdir:
            permanent = True
            util.try_makedirs(outdir)
        else:
            permanent = False

        if not parter:
            parter = self.default_partition

        reduce_name = self._registry[reducer]
        map_name = self._registry[mapper]
        self._set_serializers(mapper, kwds)
        if combiner:
            combine_name = self._registry[combiner]
        else:
            combine_name = ''
        part_name = self._registry[parter]

        op = tasks.ReduceMapOperation(reduce_name, map_name, combine_name,
                part_name)
        ds = computed_data.ComputedData(op, input, splits=splits, dir=outdir,
                permanent=permanent, **kwds)
        self._manager.submit(ds)
        ds._close_callback = self._manager.close_dataset
        return ds

    def progress(self, dataset):
        """Reports the progress (fraction complete) of the given dataset."""
        return self._manager.progress(dataset)

    def _set_serializers(self, f, kwds):
        """Add any serializers specified on the given function to kwds."""
        if 'key_serializer' in kwds:
            key_s_name = kwds['key_serializer']
            del kwds['key_serializer']
        elif hasattr(f, 'key_serializer'):
            key_s_name = f.key_serializer
        else:
            key_s_name = None

        if 'value_serializer' in kwds:
            value_s_name = kwds['value_serializer']
            del kwds['value_serializer']
        elif hasattr(f, 'value_serializer'):
            value_s_name = f.value_serializer
        else:
            value_s_name = None

        names = key_s_name, value_s_name
        serializers = Serializers.from_names(names, self._program)
        kwds['serializers'] = serializers


def job_process(program_class, opts, args, default_dir, pipe,
        quit_pipe, use_bucket_server):
    """Runs user code to initialize and run a job.

    Call the user-specified run function, which will submit datasets to be
    computed.
    """
    if use_bucket_server:
        bucket_server = http.ThreadingBucketServer(('', 0), default_dir)
        _, bucket_port = bucket_server.socket.getsockname()
        bucket_proc = multiprocessing.Process(
                target=bucket_server.serve_forever, name='Bucket Server')
        bucket_proc.daemon = True
        bucket_proc.start()
        url_converter = bucket.URLConverter('', bucket_port, default_dir)
    else:
        bucket_port = None
        url_converter = None

    manager = DataManager(pipe, quit_pipe)

    user_thread = threading.Thread(target=run_user_thread,
            args=(program_class, opts, args, default_dir, manager,
                url_converter),
            name='User Thread')
    user_thread.daemon = True
    user_thread.start()

    manager.run()


def run_user_thread(program_class, opts, args, default_dir, manager,
        url_converter):
    try:
        program = program_class(opts, args)
    except Exception as e:
        logger.critical('Exception while instantiating the program: %s'
                % traceback.format_exc())
        manager.done(1)
        return

    job = Job(manager, program, opts, default_dir, url_converter)

    try:
        if opts.mrs__profile:
            exitcode = util.profile_call(program.run, (job,), {},
                    'mrs-run-user.prof')
        else:
            exitcode = program.run(job)
    except Exception as e:
        exitcode = 1
        logger.critical('Exception raised in the run function: %s'
                % traceback.format_exc())

    manager.done(exitcode)


class DataManager(object):
    """Submits datasets to and receives urls from the MapReduce implementation.

    The run method (which should be in a standalone DataManager thread)
    receives urls from the MapReduce implementation.  Other methods may be
    called from the main job thread (note that the implementation assumes that
    only one other thread will call the submit, done, close_dataset and wait
    method).
    """

    def __init__(self, pipe, quit_pipe):
        self._pipe = pipe
        self._quit_pipe = quit_pipe
        self._datasets = weakref.WeakValueDictionary()
        self._progress_dict = {}

        self._runwaitlock = threading.Lock()
        self._runwaitcv = threading.Condition(self._runwaitlock)
        self._runwaitlist = None

    def run(self):
        """Repeatedly read from the pipe."""
        poll = select.poll()
        poll.register(self._pipe, select.POLLIN)
        poll.register(self._quit_pipe, select.POLLIN)

        try:
            while True:
                for fd, event in poll.poll():
                        if fd == self._pipe.fileno():
                            message = self._pipe.recv()
                            self.handle_message(message)
                        elif fd == self._quit_pipe:
                            os.read(self._quit_pipe, 4096)
                            return
                        else:
                            assert False
        except (EOFError, KeyboardInterrupt):
            return

    def handle_message(self, message):
        if isinstance(message, BucketReady):
            try:
                ds = self._datasets[message.dataset_id]
            except KeyError:
                ds = None

            bucket = message.bucket

            if ds is not None:
                ds[bucket.source, bucket.split] = bucket
        elif isinstance(message, ProgressUpdate):
            try:
                ds = self._datasets[message.dataset_id]
            except KeyError:
                ds = None

            self._progress_dict[message.dataset_id] = message.fraction_complete
        elif isinstance(message, DatasetComputed):
            try:
                ds = self._datasets[message.dataset_id]
            except KeyError:
                ds = None

            del self._progress_dict[message.dataset_id]

            if ds is not None:
                ds.notify_urls_known()
                if message.fetched:
                    ds._fetched = True
                with self._runwaitcv:
                    ds.computation_done()
                    self._runwaitcv.notify()
        elif isinstance(message, QuitJobProcess):
            return
        else:
            assert False, 'Unknown message type.'

    def submit(self, dataset):
        """Sends the given dataset to the implementation."""
        self._datasets[dataset.id] = dataset
        if isinstance(dataset, computed_data.ComputedData):
            self._progress_dict[dataset.id] = 0.0
        # TODO: if we're running parallel PSO and the dataset is a LocalData,
        # then convert it to FileData to avoid serializing unnecessary data.
        message = DatasetSubmission(dataset)
        self._pipe.send(message)

    def done(self, exitcode=True):
        """Signals that the job is done (and the program should quit).

        The boolean value indicates whether execution was successful.
        """
        self._pipe.send(JobDone(exitcode))

    def close_dataset(self, dataset):
        """Called when a dataset is closed.  Reports this to the impl."""
        self._pipe.send(CloseDataset(dataset.id))

    def wait(self, *datasets, **kwds):
        """Wait for any of the given Datasets to complete.

        The optional timeout parameter specifies a floating point number
        of seconds to wait before giving up.  The wait function returns a
        list of datasets that are ready.
        """
        timeout = kwds.get('timeout', None)

        with self._runwaitcv:
            self._runwaitlist = datasets

            if timeout is not None:
                last_time = time.time()

            while True:
                ready_list = self._check_runwaitlist()
                if ready_list or (timeout is not None and timeout < 0):
                    break

                self._runwaitcv.wait(timeout)
                if timeout is not None:
                    now = time.time()
                    timeout -= now - last_time
            self._runwaitlist = None
        return ready_list

    def progress(self, dataset):
        """Reports the fraction of the specified dataset that is complete."""
        try:
            return self._progress_dict[dataset.id]
        except KeyError:
            return 1.0

    def _check_runwaitlist(self):
        """Finds whether any dataset in the runwaitlist is ready.

        Returns a list of all datasets that are ready or None if the
        runwaitlist is not set.  This should only be called when the
        _runwaitcv lock is held.
        """
        assert self._runwaitlock.locked()
        runwaitlist = self._runwaitlist
        if runwaitlist:
            return [ds for ds in runwaitlist if not ds.computing]
        else:
            return None


class JobToRunner(object):
    """Message from the job to the MapReduce implementation."""


class RunnerToJob(object):
    """Message from the MapReduce implementation to the job."""


class DatasetSubmission(JobToRunner):
    """Submission of a new non-computed dataset."""
    def __init__(self, ds):
        self.dataset = ds


class CloseDataset(JobToRunner):
    """Close the specified dataset, deleting all associated data."""
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id


class JobDone(JobToRunner):
    """No further datasets will be submitted and the run method is done.

    The success attribute indicates whether execution succeeded.
    """
    def __init__(self, exitcode):
        self.exitcode = exitcode


class BucketReady(RunnerToJob):
    """The given Bucket is ready."""
    def __init__(self, dataset_id, bucket):
        self.dataset_id = dataset_id
        # TODO: right now, the Serial impl sends the whole bucket with all
        # data, even if the user program doesn't need it.  Instead, there
        # should be a separate mechanism for requesting the data in the
        # serial case.
        self.bucket = bucket


class ProgressUpdate(RunnerToJob):
    def __init__(self, dataset_id, fraction_complete):
        self.dataset_id = dataset_id
        self.fraction_complete = fraction_complete


class DatasetComputed(RunnerToJob):
    """The given ComputedData set has finished being computed.

    The fetched attribute indicates whether the previously sent buckets (in
    BucketReady messages) contained data or just urls.
    """
    def __init__(self, dataset_id, fetched):
        self.dataset_id = dataset_id
        self.fetched = fetched


class QuitJobProcess(RunnerToJob):
    """The implementation has received the JobDone message and is quitting."""

# vim: et sw=4 sts=4

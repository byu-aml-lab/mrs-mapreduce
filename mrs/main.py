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

"""Mrs main method and implementations.

An Implementation defines the implementation function that will be run and
specifies its command-line options.
"""

from __future__ import division, print_function

# In this file, we perform several imports inside of methods in order to
# reduce the initial startup time (especially to make --help more pleasant).
import binascii
import logging
import multiprocessing
import os
import random
import signal
import sys
import threading
import time
import traceback
import warnings

from . import master
from . import param
from .param import ParamObj, Param
from . import runner
from . import serial
from . import util
from .version import __version__


USAGE = (""
"""%prog [OPTIONS] [ARGS]

Mrs Version """ + __version__ + """

The default implementation is Serial.  Note that you can give --help
separately for each implementation."""
)

# Set up the default random seed.  Inspired by how the random module works.
# Note that we keep the seed at 32 bits to make it manageable.
SEED_BYTES = 4
SEED_BITS = 8 * SEED_BYTES
try:
    DEFAULT_SEED = int(binascii.hexlify(os.urandom(SEED_BYTES)), 16)
except NotImplementedError:
    import time
    DEFAULT_SEED = hash(time.time())

logger = logging.getLogger('mrs')


def main(program_class, update_parser=None, args=None):
    """Run a MapReduce program.

    Requires a program class (which may inherit from mrs.MapReduce).  The
    optional `args` attribute, if specified, is used instead of `sys.argv`.
    The `update_parser` parameter is deprecated.

    In its simplest form, a program class must have an `__init__` method
    taking `opts` and `args` parameters and a `run` method taking a `job`
    parameter, though `mrs.MapReduce` defines a default higher-level
    interface.  If you want to modify the default Mrs Parser, provide an
    update_parser classmethod on the program_class that takes a parser and
    either modifies it or returns a new one.  Note that no command-line option
    should ever have the value None because None cannot be serialized and sent
    over the network.
    """
    parser = option_parser()
    if hasattr(program_class, 'update_parser'):
        parser = program_class.update_parser(parser)
    if update_parser:
        warnings.warn('The update_parser argument is deprecated.',
                DeprecationWarning)
        parser = update_parser(parser)
    opts, args = parser.parse_args(args)

    mrs_impl = param.instantiate(opts, 'mrs')
    mrs_impl.program_class = program_class

    try:
        exitcode = mrs_impl.main(opts, args)
        sys.exit(exitcode)
    except KeyboardInterrupt:
        logger.critical('Quitting due to keyboard interrupt.')
        sys.exit(1)


def option_parser():
    """Create the default Mrs Parser

    The parser is a param.OptionParser.  It is configured to use the
    resolve conflict_handler, so any option can be overridden simply by
    defining a new option with the same option string.  The remove_option and
    get_option methods still work, too.  Note that overriding an option only
    shadows it while still allowing its other option strings to work, but
    remove_option completely removes the option with all of its option
    strings.

    The usage string can be specified with set_usage, thus overriding the
    default.  However, often what you really want to set is the epilog.  The
    usage shows up in the help before the option list; the epilog appears
    after.
    """
    parser = param.OptionParser(conflict_handler='resolve')
    parser.usage = USAGE
    parser.add_option('-I', '--mrs', dest='mrs', metavar='IMPLEMENTATION',
            action='extend', search=['mrs.main'], default='Serial',
            help='Mrs Implementation (Serial, Master, Slave, Bypass, etc.)')

    return parser


class BaseImplementation(ParamObj):
    """The base implementation.

    This needs to be extended to be useful.
    """

    _params = dict(
        verbose=Param(type='bool', doc='Verbose mode (set log level to INFO)'),
        debug=Param(type='bool', doc='Debug mode (set log level to DEBUG)'),
        profile=Param(type='bool', doc='Run the python profiler'),
        # Seed needs to be a string to avoid triggering XMLRPC limits:
        seed=Param(default=str(DEFAULT_SEED),
            doc='Random seed, default changes each run'),
        timing_file=Param(doc='Name of a file to write timing data to')
        )

    def __init__(self):
        ParamObj.__init__(self)

    def main(self, opts=None, args=None):
        if opts is None:
            opts = object()
        if args is None:
            args = []

        start_time = time.time()

        if self.debug:
            logger.setLevel(logging.DEBUG)
        elif self.verbose:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.WARNING)

        try:
            return self._main(opts, args)
        finally:
            if self.timing_file:
                with open(self.timing_file, 'w') as timing_file:
                    total_time = time.time() - start_time
                    print('total_time=%s' % total_time, file=timing_file)

    def _main(self, opts, args):
        """Method to be overridden by subclasses."""
        raise NotImplementedError('Implementation must be extended.')

    def make_job_process(self, opts, args, jobdir=None):
        """Creates a job process.

        Returns a (process, connection) pair.
        """
        from . import job

        job_conn, child_job_conn = multiprocessing.Pipe()
        child_job_quit_pipe, job_quit_pipe = os.pipe()
        job_proc = multiprocessing.Process(target=job.job_process,
                name='Job Process',
                args=(self.program_class, opts, args, jobdir, child_job_conn,
                    child_job_quit_pipe, self.use_bucket_server))
        return job_proc, job_conn, job_quit_pipe

    def start_worker_process(self, profile):
        from . import worker

        self.worker_pipe, worker_pipe2 = multiprocessing.Pipe()

        w = worker.Worker(self.program_class, worker_pipe2)
        if profile:
            target = w.profiled_run
        else:
            target = w.run

        worker_process = multiprocessing.Process(target=target, name='Worker')
        worker_process.start()

    def stop_worker_process(self):
        if self.worker_pipe is not None:
            from . import worker
            self.worker_pipe.send(worker.WorkerQuitRequest())


class Bypass(BaseImplementation):
    """Runs a program, bypassing the MapReduce functions."""

    def _main(self, opts, args):
        program = self.program_class(opts, args)
        return program.bypass()


class Implementation(BaseImplementation):
    """A general implementation referring to an overridable runner class."""

    runner_class = None
    runner = None
    shared = None
    use_bucket_server = False

    def _main(self, opts, args):
        from . import job
        from . import runner
        from . import util

        if self.runner_class is None:
            raise NotImplementedError('Subclasses must set runner_class.')

        if self.shared:
            jobdir = util.mktempdir(self.shared, 'mrs.job_')
            self.use_bucket_server = False
            default_dir = os.path.join(jobdir, 'user_run')
            os.mkdir(default_dir)
        elif self.tmpdir:
            jobdir = ''
            util.try_makedirs(self.tmpdir)
            default_dir = util.mktempdir(self.tmpdir, 'mrs_master_')
        else:
            jobdir = None
            default_dir = None

        job_proc, job_conn, job_quit_pipe = self.make_job_process(
                opts, args, default_dir)
        try:
            job_proc.start()

            # Install a signal handler for debugging.
            signal.signal(signal.SIGUSR1, self.sigusr1_handler)
            signal.siginterrupt(signal.SIGUSR1, False)

            self.start_worker_process(opts.mrs__profile)

            self.runner = self.runner_class(self.program_class, opts, args,
                    job_conn, jobdir, default_dir, self.worker_pipe)

            if opts.mrs__profile:
                exitcode = util.profile_call(self.runner.run, (), {},
                        'mrs-runner.prof')
            else:
                exitcode = self.runner.run()
        except KeyboardInterrupt:
            logger.critical('Quitting due to keyboard interrupt.')
            exitcode = 1
        finally:
            os.write(job_quit_pipe, b'\0')
            self.stop_worker_process()

        # Clean up jobdir
        if not self.keep_tmp:
            if jobdir:
                util.remove_recursive(jobdir)
            elif default_dir:
                util.remove_recursive(default_dir)
        return exitcode

    def sigusr1_handler(self, signum, stack_frame):
        # Apparently the setting siginterrupt can get reset on some platforms.
        signal.siginterrupt(signal.SIGUSR1, False)
        print('Received SIGUSR1. Current stack trace:', file=sys.stderr)
        traceback.print_stack(stack_frame)
        if self.runner is not None:
            self.runner.debug_status()


class Serial(Implementation):
    """Runs a MapReduce job in serial."""

    runner_class = serial.SerialRunner
    keep_tmp = False
    tmpdir = None

    def start_worker_process(self, profile):
        """Do-nothing method (no worker needed in the serial impl)."""
        self.worker_pipe = None


class FileParams(ParamObj):
    _params = dict(
        tmpdir=Param(default='/var/tmp', doc='Local temporary storage'),
        keep_tmp=Param(type='bool',
            doc="Do not delete temporary files at completion"),
        )


class TaskRunnerParams(ParamObj):
    _params = dict(
        shared=Param(doc='Global shared area for temporary storage (optional)'),
        reduce_tasks=Param(default=1, type='int',
            doc='Default number of reduce tasks'),
        timing_interval=Param(default=0, type='float',
            doc="Interval (seconds) between outputting timing statistics"),
        sequential_datasets=Param(type='bool',
            doc="Compute datasets sequentially (for performance comparisons)"),
        )


class MockParallel(Implementation, FileParams, TaskRunnerParams):
    """MapReduce execution on POSIX shared storage, such as NFS.

    This creates all of the tasks that are used in the normal parallel
    implementation, but it executes them in serial.  This can make debugging a
    little easier.

    Note that progress times often seem wrong in mockparallel.  The reason is
    that most of the execution time is in I/O, and mockparallel tries to load
    the input for all reduce tasks before doing the first reduce task.
    """
    runner_class = runner.MockParallelRunner


class NetworkParams(ParamObj):
    _params = dict(
        port=Param(default=0, type='int', shortopt='-P',
            doc='RPC Port for incoming requests'),
        timeout=Param(default=20, type='float',
            doc='Timeout for RPC calls (incl. pings)'),
        pingdelay=Param(default=30, type='float',
            doc='Interval between pings'),
        )


class Master(Implementation, FileParams, NetworkParams, TaskRunnerParams):
    _params = dict(
        runfile=Param(default='',
            doc="Server's RPC port will be written here"),
        )

    runner_class = master.MasterRunner
    use_bucket_server = True

    def start_worker_process(self, profile):
        """Do-nothing method (no worker needed in the master)."""
        self.worker_pipe = None


class Slave(BaseImplementation, FileParams, NetworkParams):
    _params = dict(
        master=Param(shortopt='-M', doc='URL of the Master RPC server'),
        )

    def _main(self, opts, args):
        """Run Mrs Slave

        Slave Main is called directly from Mrs Main.  On exit, the process
        will return slave_main's return value.
        """
        from . import slave

        if not self.master:
            logger.critical('No master URL specified.')
            return 1

        self.start_worker_process(opts.mrs__profile)

        s = slave.Slave(self.program_class, self.master, self.tmpdir,
                self.pingdelay, self.timeout, self.worker_pipe)
        try:
            exitcode = s.run()
        finally:
            self.stop_worker_process()
        return exitcode

# vim: et sw=4 sts=4

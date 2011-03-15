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

"""Mrs main method and implementations.

An Implementation defines the implementation function that will be run and
specifies its command-line options.
"""

# In this file, we perform several imports inside of methods in order to
# reduce the initial startup time (especially to make --help more pleasant).
import binascii
import os
import random
import signal
import sys

from . import master
from . import runner
from . import serial
from .param import ParamObj, Param
from .version import VERSION


USAGE = (""
"""%prog [OPTIONS] [ARGS]

Mrs Version """ + VERSION + """

The default implementation is Serial.  Note that you can give --help
separately for each implementation."""
)

# Set up the default random seed.  Inspired by how the random module works.
# Note that we keep the seed at 32 bits to make it manageable.
SEED_BITS = 32
try:
    seed_bytes = int(SEED_BITS / 8)
    DEFAULT_SEED = int(binascii.hexlify(os.urandom(seed_bytes)), 16)
except NotImplementedError:
    import time
    DEFAULT_SEED = hash(time.time())

DEFAULT_SHARED = os.getcwd()

import logging
logger = logging.getLogger('mrs')
del logging


def main(program_class, update_parser=None):
    """Run a MapReduce program.

    Requires a program class (which inherits from mrs.MapReduce) and an
    optional update_parser function.

    If you want to modify the basic Mrs Parser, provide an update_parser
    function that takes a parser and either modifies it or returns a new one.
    Note that no option should ever have the value None.
    """
    import param

    parser = option_parser()
    if update_parser:
        parser = update_parser(parser)
    opts, args = parser.parse_args()

    mrs_impl = param.instantiate(opts, 'mrs')
    mrs_impl.program_class = program_class

    try:
        mrs_impl.main(opts, args)
        sys.exit(0)
    except KeyboardInterrupt:
        logger.critical('Quitting due to keyboard interrupt.')
        print >>sys.stderr, "Interrupted."
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
    import param

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
        # Seed needs to be a string to avoid triggering XMLRPC limits:
        seed=Param(default=str(DEFAULT_SEED),
            doc='Random seed, default changes each run'),
        )

    def __init__(self):
        ParamObj.__init__(self)

    def main(self, opts=None, args=None):
        if opts is None:
            opts = object()
        if args is None:
            args = []

        if self.debug:
            logger.setLevel(logging.DEBUG)
        elif self.verbose:
            logger.setLevel(logging.INFO)

        self._main(opts, args)

    def _main(self, opts, args):
        """Method to be overridden by subclasses."""
        raise NotImplementedError('Implementation must be extended.')

    def make_job_process(self, opts, args, jobdir=None):
        """Creates a job process.

        Returns a (process, connection) pair.
        """
        import multiprocessing
        from . import job

        job_conn, child_job_conn = multiprocessing.Pipe()
        job_proc = multiprocessing.Process(target=job.job_process,
                name='Job',
                args=(self.program_class, opts, args, jobdir, child_job_conn))
        return job_proc, job_conn


class Bypass(BaseImplementation):
    """Runs a program, bypassing the MapReduce functions."""

    def _main(self, opts, args):
        program = self.program_class(opts, args)
        program.bypass()


class Implementation(BaseImplementation):
    """A general implementation referring to an overridable runner class."""

    runner_class = None
    runner = None
    shared = None
    keep_jobdir = False

    def _main(self, opts, args):
        from . import job
        from . import runner

        if self.runner_class is None:
            raise NotImplementedError('Subclasses must set runner_class.')

        jobdir = self.make_jobdir(opts)
        job_proc, job_conn = self.make_job_process(opts, args, jobdir)
        job_proc.daemon = True
        job_proc.start()

        # Install a signal handler for debugging.
        signal.signal(signal.SIGUSR1, self.sigusr1_handler)
        signal.siginterrupt(signal.SIGUSR1, False)

        self.runner = self.runner_class(self.program_class, opts, args,
                job_conn, jobdir)
        try:
            self.runner.run()
        except KeyboardInterrupt:
            logger.critical('Quitting due to keyboard interrupt.')

        # Clean up jobdir
        if jobdir and not self.keep_jobdir:
            from util import remove_recursive
            remove_recursive(jobdir)

    def make_jobdir(self, opts):
        """Make a temporary job directory, if appropriate."""
        import tempfile
        if self.shared:
            jobdir = tempfile.mkdtemp(prefix='mrs.job_', dir=self.shared)
        else:
            jobdir = None
        return jobdir

    def sigusr1_handler(self, signum, stack_frame):
        # Apparently the setting siginterrupt can get reset on some platforms.
        signal.siginterrupt(signal.SIGUSR1, False)
        if self.runner is not None:
            self.runner.debug_status()


class Serial(Implementation):
    """Runs a MapReduce job in serial."""

    runner_class = serial.SerialRunner


class MockParallel(Implementation):
    """MapReduce execution on POSIX shared storage, such as NFS.

    This creates all of the tasks that are used in the normal parallel
    implementation, but it executes them in serial.  This can make debugging a
    little easier.

    Note that progress times often seem wrong in mockparallel.  The reason is
    that most of the execution time is in I/O, and mockparallel tries to load
    the input for all reduce tasks before doing the first reduce task.
    """
    _params = dict(
        shared=Param(default=DEFAULT_SHARED,
            doc='Global shared area for temporary storage'),
        keep_jobdir=Param(type='bool',
            doc="Do not delete jobdir at completion"),
        reduce_tasks=Param(default=1, type='int',
            doc='Default number of reduce tasks'),
        )

    runner_class = runner.MockParallelRunner


class Network(ParamObj):
    _params = dict(
        port=Param(default=0, type='int', shortopt='-P',
            doc='RPC Port for incoming requests'),
        timeout=Param(default=20, type='float',
            doc='Timeout for RPC calls (incl. pings)'),
        pingdelay=Param(default=5, type='float',
            doc='Interval between pings'),
        )


class Master(Implementation, Network):
    _params = dict(
        shared=Param(default=DEFAULT_SHARED,
            doc='Global shared area for temporary storage'),
        keep_jobdir=Param(type='bool',
            doc="Do not delete jobdir at completion"),
        reduce_tasks=Param(default=1, type='int',
            doc='Default number of reduce tasks'),
        runfile=Param(default='',
            doc="Server's RPC port will be written here"),
        )

    runner_class = master.MasterRunner


class Slave(BaseImplementation, Network):
    _params = dict(
        master=Param(shortopt='-M', doc='URL of the Master RPC server'),
        local_shared=Param(default=None,
            doc='Local shared area for temporary storage'),
        )

    def _main(self, opts, args):
        """Run Mrs Slave

        Slave Main is called directly from Mrs Main.  On exit, the process
        will return slave_main's return value.
        """
        from multiprocessing import Process, Pipe
        from threading import Thread
        from slave import Slave, SlaveInterface
        from worker import run_worker
        import registry
        import rpc
        program_hash = registry.object_hash(self.program_class)

        if not self.master:
            logger.critical('No master URL specified.')
            return 1
        slave = Slave(program_hash, self.master, self.local_shared,
                self.pingdelay, self.timeout)

        rpc_interface = SlaveInterface(slave)
        rpc_server = rpc.Server(('', 0), rpc_interface)
        _, slave.port = rpc_server.socket.getsockname()

        # Create and start the other processes:
        worker = Process(name='Worker', target=run_worker,
                args=(self.program_class, slave.request_pipe_worker))
        worker.start()

        # Create and start the other threads:
        slave_thread = Thread(name='Slave', target=slave.run)
        slave_thread.daemon = True
        slave_thread.start()

        rpc_thread = Thread(name='RPC Server', target=rpc_server.serve_forever)
        rpc_thread.daemon = True
        rpc_thread.start()

        try:
            # KeyboardInterrupt can't interrupt waiting on a lock unless
            # a timeout is given, so we do a loop:
            while slave_thread.is_alive():
                slave_thread.join(10)
        except KeyboardInterrupt:
            slave.quit()
            while slave_thread.is_alive():
                slave_thread.join(10)

        worker.terminate()

# vim: et sw=4 sts=4

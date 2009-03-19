# Mrs
# Copyright 2008-2009 Brigham Young University
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

"""Mrs Implementations

An Implementation defines the implementation function that will be run and 
specifies its command-line options.
"""

from param import ParamObj, Param
import binascii
import random
import os
import sys

import logging
logger = logging.getLogger('mrs')
del logging

# Set up the default random seed.  Inspired by how the random module works.
# Note that we keep the seed at 32 bits to make it manageable.
SEED_BITS = 32
try:
    seed_bytes = int(SEED_BITS / 8)
    DEFAULT_SEED = int(binascii.hexlify(os.urandom(seed_bytes)), 16)
except NotImplementedError:
    import time
    DEFAULT_SEED = hash(time.time())


class Implementation(ParamObj):
    """The base implementation.

    This needs to be extended to be useful.
    """

    _params = dict(
        verbose=Param(type='bool', doc='Verbose mode (set log level to INFO)'),
        debug=Param(type='bool', doc='Debug mode (set log level to DEBUG)'),
        seed=Param(default=DEFAULT_SEED, type='int',
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


class Bypass(Implementation):
    """Runs a program, bypassing the MapReduce functions."""

    def _main(self, opts, args):
        program = self.program_class(opts, args)
        program.bypass()


class Serial(Implementation):
    """Runs a MapReduce job in serial."""

    def __init__(self):
        Implementation.__init__(self)

        import threading
        self.cv = threading.Condition()

    def _main(self, opts, args):
        from job import Job

        self.job = Job(self.program_class, opts, args)
        self.job.update_callback = self.job.end_callback = self.job_updated
        self.job.start()

        while self.ready():
            dataset = self.job.active_data[0]
            dataset.run_serial()
            self.job.check_done()

        self.job.join()

    def ready(self):
        """Waits for a dataset to become ready.

        Returns True when a dataset is ready and False when the job is
        finished.
        """
        # A quick optimization for jobs with lots of datasets:
        if self.job.active_data:
            return True

        self.cv.acquire()
        try:
            while True:
                if self.job.active_data:
                    return True
                elif self.job.done():
                    return False
                else:
                    self.cv.wait()
        finally:
            self.cv.release()

    def job_updated(self):
        """Called when the job is updated or completed.
        
        Called from another thread.
        """
        self.cv.acquire()
        self.cv.notify()
        self.cv.release()


class MockParallel(Implementation):
    """MapReduce execution on POSIX shared storage, such as NFS.
    
    This creates all of the tasks that are used in the normal parallel
    implementation, but it executes them in serial.  This can make debugging a
    little easier.

    Note that progress times often seem wrong in mockparallel.  The reason is
    that most of the execution time is in I/O, and mockparallel tries to load
    the input for all reduce tasks before doing the first reduce task.
    """
    def _main(self, opts, args):
        raise NotImplementedError("The mockparallel implementation is"
                "temporarily broken.  Sorry.")

        from job import Job
        from util import try_makedirs

        # Set up shared directory
        try_makedirs(self.shared)

        job = Job(self.program_class, opts, args)
        job.start()

        while not job.done():
            task = job.schedule()
            # FIXME (busy loop):
            if task is None:
                continue
            task.active()
            task.run()
            task.finished()
            job.check_done()

        job.join()


class Network(Implementation):
    _params = dict(
        port=Param(default=0, type='int', shortopt='-P',
            doc='RPC Port for incoming requests'),
        timeout=Param(default=20, type='float',
            doc='Timeout for RPC calls (incl. pings)'),
        pingdelay=Param(default=5, type='float',
            doc='Interval between pings'),
        )


class Master(Network):
    import os
    default_shared = os.getcwd()
    _params = dict(
        shared=Param(default=default_shared,
            doc='Shared area for temporary storage'),
        keep_jobdir=Param(type='bool',
            doc="Do not delete jobdir at completion"),
        reduce_tasks=Param(default=1, type='int',
            doc='Default number of reduce tasks'),
        runfile=Param(doc="Server's RPC port will be written here"),
        )

    def _main(self, opts, args):
        """Run Mrs Master

        Master Main is called directly from Mrs Main.  On exit, the process
        will return master_main's return value.
        """
        from job import Job
        from io import blocking
        from master import MasterState, MasterEventThread

        # create job thread:
        job = Job(self.program_class, opts, args)
        # create master state:
        import registry
        program_hash = registry.object_hash(self.program_class)
        master = MasterState(job, program_hash, opts, args)
        # create event thread:
        event_thread = MasterEventThread(master)
        # create blocking thread (which is only started if necessary):
        job.blockingthread = blocking.BlockingThread()

        # Start the other threads:
        event_thread.start()
        job.start()

        try:
            # Note: under normal circumstances, the reactor (in the event
            # thread) will quit on its own.
            master.reaper.wait()
        except KeyboardInterrupt:
            pass

        event_thread.shutdown()

        # Clean up jobdir
        if not self.keep_jobdir:
            from util import remove_recursive
            remove_recursive(job.jobdir)

        # Wait for event thread to finish.
        event_thread.join()

        if master.reaper.traceback:
            logger.critical('Exception: %s' % master.reaper.traceback)


class Slave(Network):
    _params = dict(
        master=Param(shortopt='-M', doc='URL of the Master RPC server'),
        )

    def _main(self, opts, args):
        """Run Mrs Slave

        Slave Main is called directly from Mrs Main.  On exit, the process
        will return slave_main's return value.
        """
        from slave import SlaveState, SlaveEventThread, Worker
        import registry
        program_hash = registry.object_hash(self.program_class)
        slave = SlaveState(program_hash, self.master, self.pingdelay,
                self.timeout)

        # Create the other threads:
        worker = Worker(slave, self.program_class)
        event_thread = SlaveEventThread(slave)

        # Start the other threads:
        event_thread.start()
        worker.start()

        try:
            # Note: under normal circumstances, the reactor (in the event
            # thread) will quit on its own.
            slave.reaper.wait()
        except KeyboardInterrupt:
            pass

        event_thread.shutdown()
        event_thread.join()

        if slave.reaper.traceback:
            logger.error('Exception: %s' % slave.reaper.traceback)



# vim: et sw=4 sts=4

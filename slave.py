# Mrs
# Copyright 2008 Brigham Young University
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
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

"""Mrs Slave

The Mrs Slave runs in three threads: the main thread, the event thread, and
the worker thread.

The main thread doesn't really do anything.  It starts the other two threads
and waits for the other two to finish.  If the user hits CTRL-C, the main
thread will be interrupted, and it will shut down the event thread.  The only
reason that the main thread exists at all is to deal with signals.

The event thread does all of the work.  It connects to the server, listens for
RPC requests, and downloads files.  Its control flow is asynchronous.  This
can make some things easier to understand and other things harder to
understand.  It's a tradeoff, but in the end, it's easier and more efficient
to deal with network IO asynchronously than with a heap of threads.  Anyway,
the event loop is managed by the Twisted reactor.

The worker thread executes the user's map function and reduce function.
That's it.  It just does what the event thread tells it to.
"""

COOKIE_LEN = 8
SLAVE_PING_INTERVAL = 5.0
QUIT_DELAY = 0.5

import threading
from twisted.internet import reactor
from twist import TwistedThread, GrimReaper
from twistrpc import RequestXMLRPC


def slave_main(registry, user_run, user_setup, args, opts):
    """Run Mrs Slave

    Slave Main is called directly from Mrs Main.  On exit, the process
    will return slave_main's return value.
    """

    from twistrpc import FromThreadProxy
    # Create an RPC proxy to the master's RPC Server.  This will be used
    # mostly from the Worker thread.
    master = FromThreadProxy(opts.mrs_master)

    slave = Slave(registry, user_setup, master)

    reaper = GrimReaper()

    # Create threads.
    worker = Worker(slave, master, reaper)
    event_thread = SlaveEventThread(slave, worker, reaper)

    # Start the other threads.
    event_thread.start()
    worker.start()

    try:
        # Note: under normal circumstances, the reactor (in the event
        # thread) will quit on its own.
        reaper.wait()
    except KeyboardInterrupt:
        event_thread.shutdown()

    reactor.stop()
    event_thread.join()

    if reaper.traceback:
        print reaper.traceback

    return 0


# TODO: when we stop supporting Python older than 2.5, use inlineCallbacks:
class SlaveEventThread(TwistedThread):
    """The thread that runs the Twisted reactor.
    
    We don't trust the Twisted reactor's signal handlers, so we run it with
    the handlers disabled.  As a result, it shouldn't be in the primary
    thread.
    """

    def __init__(self, slave, worker, reaper):
        TwistedThread.__init__(self)
        self.slave = slave
        self.worker = worker
        self.reaper = reaper

    def die(self, message):
        import sys
        print >>sys.stderr, message
        self.reaper.reap()

    def run(self):
        """Called when the TwistedThread is first initialized.
        
        It starts the reactor and schedules signin() to be called.
        """
        reactor.callLater(0, self.signin)
        TwistedThread.run(self)

        # Let other threads know that we are quitting.
        self.reaper.reap()

    # state 1
    def signin(self):
        """Start Slave RPC Server and sign in to master."""
        from twisted.web import server

        resource = SlaveInterface(self.slave, self.worker)
        site = server.Site(resource)
        tcpport = reactor.listenTCP(0, site)
        address = tcpport.getHost()

        # Initiate signin to master
        from version import VERSION
        cookie = self.slave.cookie
        port = address.port
        source_hash = self.slave.registry.source_hash()
        reg_hash = self.slave.registry.reg_hash()

        master = self.slave.master
        deferred = master.callRemote('signin', VERSION, cookie, port,
                source_hash, reg_hash)

        deferred.addCallbacks(self.signin_callback, self.signin_errback)

    def signin_errback(self, failure):
        print failure
        self.die('Unable to contact master.')

    # state 2
    def signin_callback(self, value):
        """Process the results from the signin.

        This is the callback after signin.  After saving the return values,
        schedule the user_setup function to be run in the Worker.
        """
        slave_id, optdict = value

        if slave_id < 0:
            self.die('Master rejected signin.')

        # Save the slave id given by the master.
        self.slave.id = slave_id

        # Tell the Worker to run the user_setup function.
        import optparse
        options = optparse.Values(optdict)
        callback = self.report_ready
        self.worker.start_setup(options, callback)

    def report_ready(self):
        """Report to the master that we are ready to accept tasks.
        
        This is the callback after user_setup is called.
        """

        # Report for duty.
        master = self.slave.master
        master.callRemote('ready', self.slave.id, self.slave.cookie)


# TODO: ADD A PING TASK BACK IN HERE!!!
    #master_alive = self.master.blocking_call('ping', self.id, self.cookie)


class Slave(object):
    """Mrs Slave"""

    def __init__(self, registry, user_setup, master):
        self.master = master
        self.registry = registry
        self.user_setup = user_setup

        self.id = None
        self.alive = True
        self.cookie = self.rand_cookie()

    @classmethod
    def rand_cookie(cls):
        # Generate a cookie so that mostly only authorized people can connect.
        from random import choice
        import string
        possible = string.letters + string.digits
        return ''.join(choice(possible) for i in xrange(COOKIE_LEN))

    def check_cookie(self, cookie):
        if cookie != self.cookie:
            raise CookieValidationError


class SlaveInterface(RequestXMLRPC):
    """Public XML RPC Interface
    
    Note that any method not beginning with "xmlrpc_" will be exposed to
    remote hosts.  Any of these can return either a result or a deferred.
    """
    def __init__(self, slave, worker):
        RequestXMLRPC.__init__(self)
        self.slave = slave
        self.worker = worker

    def xmlrpc_start_map(self, taskid, inputs, func_name, part_name, nparts,
            output, extension, cookie):
        self.slave.check_cookie(cookie)
        return self.worker.start_map(taskid, inputs, func_name,
                part_name, nparts, output, extension)

    def xmlrpc_start_reduce(self, taskid, inputs, func_name, part_name,
            nparts, output, extension, cookie):
        return self.worker.start_reduce(taskid, inputs, func_name,
                part_name, nparts, output, extension)

    def xmlrpc_quit(self, cookie):
        self.slave.check_cookie(cookie)
        self.slave.alive = False
        import sys
        print >>sys.stderr, "Quitting as requested by an RPC call."
        # We delay before actually stopping because we need to make sure that
        # the response gets sent back.
        reaper = self.worker.reaper
        reactor.callLater(QUIT_DELAY, lambda: reaper.reap())
        return True

    def xmlrpc_ping(self):
        """Master checking if we're still here.
        """
        return True


class CookieValidationError(Exception):
    pass


class Worker(threading.Thread):
    """Execute map tasks and reduce tasks.

    The worker waits for other threads to make assignments by calling
    start_map and start_reduce.

    This needs to run in a daemon thread rather than in the main thread so
    that it can be killed by other threads.
    """
    def __init__(self, slave, master, reaper):
        threading.Thread.__init__(self)
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

        self.slave = slave
        self.master = master
        self.reaper = reaper

        self._task = None
        self._cond = threading.Condition()
        self.active = False
        self.exception = None

        self.options = None
        self._setup_ready = threading.Event()
        self._setup_callback = None

    def start_setup(self, options, callback):
        """Start running the user_setup function.
        
        The given callback function will be invoked in the reactor thread.
        """
        self.options = options
        self._setup_ready.set()
        self._setup_callback = callback

    def start_map(self, taskid, inputs, map_name, part_name, nparts, output,
            extension):
        """Tell this worker to start working on a map task.

        This will ordinarily be called from some other thread.
        """
        from task import MapTask
        from datasets import FileData
        from io import writerformat

        input_data = FileData(inputs, splits=1)
        format = writerformat(extension)

        success = False
        self._cond.acquire()
        if self._task is None:
            registry = self.slave.registry
            self._task = MapTask(taskid, input_data, 0, map_name, part_name,
                    nparts, output, format, registry)
            success = True
            self._cond.notify()
        self._cond.release()
        return success

    def start_reduce(self, taskid, inputs, reduce_name, part_name, nparts,
            output, extension):
        """Tell this worker to start working on a reduce task.

        This will ordinarily be called from some other thread.
        """
        from task import ReduceTask
        from datasets import FileData
        from io import writerformat

        input_data = FileData(inputs, splits=1)
        format = writerformat(extension)

        success = False
        self._cond.acquire()
        if self._task is None:
            registry = self.slave.registry
            self._task = ReduceTask(taskid, input_data, 0, reduce_name,
                    part_name, nparts, output, format, registry)
            success = True
            self._cond.notify()
        self._cond.release()
        return success

    def run(self):
        """Run the worker."""

        # Run user_setup if requested:
        self._setup_ready.wait()
        user_setup = self.slave.user_setup
        if user_setup:
            try:
                user_setup(self.options)
            except Exception, e:
                print "Caught an exception in the Worker thread (in setup)!"
                self.reaper.reap(e)
                return

        # Alert the Twisted thread that user_setup is done.
        reactor.callFromThread(self._setup_callback)

        # Process tasks:
        while True:
            self._cond.acquire()
            while self._task is None:
                self._cond.wait()
            task = self._task
            self._cond.release()

            try:
                task.run()
            except Exception, e:
                print "Caught an exception in the Worker thread!"
                self.reaper.reap(e)
                return

            self._cond.acquire()
            self._task = None
            self._cond.release()

            # TODO: right now, dataset.dump() happens in task.run().  Instead,
            # we should tell the dataset to become available() here, and the
            # data should automatically be dumped.
            self.master.blocking_call('done', self.slave.id, task.outurls(),
                    self.slave.cookie)

            # TODO: Catch ErrbackException here


# vim: et sw=4 sts=4

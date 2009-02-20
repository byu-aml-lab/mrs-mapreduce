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
That's it.  It just does what the event thread tells it to.  Note that since
the worker thread is a daemon thread, it quits implicitly once the other two
threads complete.
"""

# Number of ping timeouts before giving up:
PING_ATTEMPTS = 50
WATCHDOG_INTERVAL = 5
WATCHDOG_TIMEOUT = 2

COOKIE_LEN = 8
QUIT_DELAY = 0.5

import threading
from twisted.internet import reactor, defer
from twist import TwistedThread, GrimReaper, PingTask, ErrbackException
from twistrpc import RequestXMLRPC, TimeoutProxy

from logging import getLogger
logger = getLogger('mrs')


def slave_main(registry, user_run, user_setup, args, opts):
    """Run Mrs Slave

    Slave Main is called directly from Mrs Main.  On exit, the process
    will return slave_main's return value.
    """

    slave = Slave(registry, user_setup, opts)

    # Create the other threads:
    worker = Worker(slave)
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


# TODO: when we stop supporting Python older than 2.5, use inlineCallbacks:
class SlaveEventThread(TwistedThread):
    """Thread on slave that runs the Twisted reactor
    
    We don't trust the Twisted reactor's signal handlers, so we run it with
    the handlers disabled.  As a result, it shouldn't be in the primary
    thread.
    """

    def __init__(self, slave):
        TwistedThread.__init__(self)
        self.slave = slave

    def run(self):
        """Called when the thread is started.
        
        It starts the reactor and schedules signin() to be called.
        """
        reactor.callLater(0, self.slave.signin)
        TwistedThread.run(self)

        # Let other threads know that we are quitting.
        self.slave.quit()


class Slave(object):
    """State of a Mrs slave
    
    Since execution flow is event-driven, a little overview might be helpful.
    When the slave starts up, the `signin` method is called.  If it fails, the
    `signin_errback` method shuts everything down, but if it succeeds, the
    `signin_callback` method starts some initialization in the Worker thread.
    When this initialization finishes, the Worker thread will trigger the
    `report_ready` method, which causes the slave to report in to the master.
    Later on, the Worker may trigger this method again if it can't contact the
    master; this gives us a chance to reconnect if the network hiccups for a
    few minutes.
    """

    def __init__(self, registry, user_setup, opts):
        self.mrs_master = opts.mrs_master
        self.registry = registry
        self.user_setup = user_setup
        self.pingdelay = opts.mrs_pingdelay

        self.reaper = GrimReaper()
        self.worker = None
        self.ping_task = None
        self.timeouts = 0

        self.master_rpc = TimeoutProxy(self.mrs_master, opts.mrs_timeout)

        self.id = None
        self.alive = True
        self.cookie = self.rand_cookie()
        self.timestamp = None
        self.watchdog_stamp = None

    # State 1
    def signin(self):
        """Start Slave RPC Server and sign in to master."""
        from twisted.web import server

        resource = SlaveInterface(self)
        site = server.Site(resource)
        tcpport = reactor.listenTCP(0, site)
        address = tcpport.getHost()

        # Initiate signin to master
        from version import VERSION
        cookie = self.cookie
        port = address.port
        source_hash = self.registry.source_hash()
        reg_hash = self.registry.reg_hash()

        deferred = self.master_rpc.callRemote('signin', VERSION, cookie, port,
                source_hash, reg_hash)

        deferred.addCallbacks(self.signin_callback, self.signin_errback)

    def signin_errback(self, failure):
        logger.error('Unable to contact master: %s' % failure)
        self.quit()

    # State 2
    def signin_callback(self, value):
        """Process the results from the signin.

        This is the callback after signin.  After saving the return values,
        schedule the user_setup function to be run in the Worker.
        """
        slave_id, optdict = value

        if slave_id < 0:
            self.quit('Master rejected signin.')

        # Save the slave id given by the master.
        self.id = slave_id

        self.run_watchdog()

        ping_args = ('ping', self.id, self.cookie)
        self.ping_task = PingTask(self.pingdelay, ping_args, self.master_rpc,
                self.ping_success, self.ping_failure, self.get_timestamp)

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
        deferred = self.master_rpc.callRemote('ready', self.id, self.cookie)
        deferred.addCallback(self.ready_success)
        deferred.addErrback(self.ready_failure)

    def ready_success(self, value):
        """Called when reporting in as ready succeeded."""
        logger.info('Connected to master.')
        self.update_timestamp()
        self.timeouts = 0
        if not self.ping_task.running:
            self.ping_task.start()

    def ready_failure(self, err):
        """Called when reporting in as ready failed."""
        give_up = False
        if err.check(defer.TimeoutError):
            self.timeouts += 1
            if self.timeouts > PING_ATTEMPTS:
                logger.critical('Too many timeouts.  Giving up.')
                give_up = True
            else:
                # Try to connect again
                logger.error('Reconnect timed out.  Trying again.')
                self.report_ready()
        else:
            logger.critical('Failed to report due to network error: %s' % err)
            give_up = True

        if give_up:
            self.reaper.reap()

    def ping_failure(self, err):
        """Report that the master failed to respond to an RPC request.

        This may be either a ping or some other request.  At the moment,
        we aren't very lenient, but in the future we could allow a few
        failures before disconnecting.
        """
        give_up = False
        if err.check(defer.TimeoutError):
            self.timeouts += 1
            if self.timeouts > PING_ATTEMPTS:
                logger.critical('Too many ping timeouts.  Giving up.')
                give_up = True
            else:
                logger.error('Ping timeout.  Trying again.')
        else:
            logger.critical('Lost master due to network error: %s' % err)

        if give_up:
            self.ping_task.stop()
            self.reaper.reap()

    def ping_success(self):
        """Called when a ping successfully completes."""
        self.update_timestamp()
        self.timeouts = 0

    def update_timestamp(self):
        """Set the timestamp to the current time."""
        from datetime import datetime
        self.timestamp = datetime.utcnow()

    def get_timestamp(self):
        """Report the most recent timestamp."""
        return self.timestamp

    def run_watchdog(self):
        from datetime import datetime, timedelta
        timeout = timedelta(seconds=(WATCHDOG_TIMEOUT))
        now = datetime.utcnow()
        stamp = self.watchdog_stamp
        if stamp is None:
            delay = timedelta()
        else:
            delay = (now - stamp) - timedelta(seconds=WATCHDOG_INTERVAL)
        if delay > timeout:
            logger.error('Watchdog alarm triggered (%s).  This means that Mrs'
                    ' is falling behind on basic communication.  This may be'
                    ' a symptom that the computer is overloaded.  However, it'
                    ' may also mean that the user map or reduce function is'
                    ' using a library that does not release the Global'
                    ' Interpreter Lock (GIL), in which case the function must'
                    ' be rewritten to use the library in a separate process.'
                    % delay)
        self.watchdog_stamp = now
        reactor.callLater(WATCHDOG_INTERVAL, self.run_watchdog)

    # Miscellaneous
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

    def register_worker(self, worker):
        """Called by the worker so the Slave can have a reference to it."""
        self.worker = worker

    def quit(self):
        """Called to quit the slave."""
        self.reaper.reap()

    def reconnect(self):
        """Called by the worker thread when it can't contact the master."""
        reactor.callFromThread(self.ping_task.stop)
        reactor.callFromThread(self.report_ready)


class SlaveInterface(RequestXMLRPC):
    """Public XML RPC Interface
    
    Note that any method not beginning with "xmlrpc_" will be exposed to
    remote hosts.  Any of these can return either a result or a deferred.
    """
    def __init__(self, slave):
        RequestXMLRPC.__init__(self)
        self.slave = slave

    def xmlrpc_start_map(self, source, inputs, func_name, part_name, splits,
            output, extension, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.debug('Received a Map assignment from the master.')
        return self.slave.worker.start_map(source, inputs, func_name,
                part_name, splits, output, extension)

    def xmlrpc_start_reduce(self, source, inputs, func_name, part_name,
            splits, output, extension, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.debug('Received a Reduce assignment from the master.')
        return self.slave.worker.start_reduce(source, inputs, func_name,
                part_name, splits, output, extension)

    def xmlrpc_quit(self, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        self.slave.alive = False
        # We delay before actually stopping because we need to make sure that
        # the response gets sent back.
        logger.info('Received a request to quit from the master.')
        reactor.callLater(QUIT_DELAY, self.slave.quit)
        return True

    def xmlrpc_ping(self, cookie):
        """Master checking if we're still here.
        """
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.debug('Received a ping from the master.')
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
    def __init__(self, slave):
        threading.Thread.__init__(self)
        self.setName('Worker')
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

        self.slave = slave
        self.slave.register_worker(self)

        from io import blocking
        self.blockingthread = blocking.BlockingThread()

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
        self._setup_callback = callback
        self._setup_ready.set()

    def start_map(self, source, inputs, map_name, part_name, splits, output,
            extension):
        """Tell this worker to start working on a map task.

        This will ordinarily be called from some other thread.
        """
        from task import MapTask
        from datasets import FileData
        from io.load import writerformat

        input_data = FileData(inputs, splits=1)
        input_data.blockingthread = self.blockingthread
        format = writerformat(extension)

        success = False
        self._cond.acquire()
        if self._task is None:
            registry = self.slave.registry
            self._task = MapTask(input_data, 0, source, map_name, part_name,
                    splits, output, format, registry)
            success = True
            self._cond.notify()
        self._cond.release()
        return success

    def start_reduce(self, source, inputs, reduce_name, part_name, splits,
            output, extension):
        """Tell this worker to start working on a reduce task.

        This will ordinarily be called from some other thread.
        """
        from task import ReduceTask
        from datasets import FileData
        from io.load import writerformat

        input_data = FileData(inputs, splits=1)
        input_data.blockingthread = self.blockingthread
        format = writerformat(extension)

        success = False
        self._cond.acquire()
        if self._task is None:
            registry = self.slave.registry
            self._task = ReduceTask(input_data, 0, source, reduce_name,
                    part_name, splits, output, format, registry)
            success = True
            self._cond.notify()
        self._cond.release()
        return success

    def run(self):
        """Run the worker."""
        # Run user_setup if requested:
        self._setup_ready.wait()
        logger.debug('Starting to run the user setup function.')
        user_setup = self.slave.user_setup
        if user_setup:
            try:
                user_setup(self.options)
            except Exception, e:
                import traceback
                logger.critical('Exception in the Worker thread (in setup): %s'
                    % traceback.format_exc())
                self.slave.quit()
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

            logger.debug('Starting to run a new task.')
            try:
                task.run()
            except Exception, e:
                import traceback
                logger.critical('Exception in the Worker thread (in run): %s'
                    % traceback.format_exc())
                self.slave.quit()
                return
            logger.debug('Task complete.')

            self._cond.acquire()
            self._task = None
            self._cond.release()

            logger.debug('Reporting task completion to the master.')
            try:
                self.slave.master_rpc.blocking_call('done', self.slave.id,
                        task.outurls(), self.slave.cookie)
            except ErrbackException, e:
                # TODO: We should be able to retry calling done, but this will
                # only be possible if done uses some unique id.  For now,
                # retrying done can be very destructive.
                logger.error('RPC error when reporting back.  Giving up.')
                # TODO: Introduce a delay, and don't reconnect forever.
                self.slave.reconnect()


# vim: et sw=4 sts=4

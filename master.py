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

"""Mrs Master

More information coming soon.
"""

import logging
import threading
from twisted.internet import reactor, defer
from twist import TwistedThread, GrimReaper, PingTask
from twistrpc import RequestXMLRPC, uses_request

logger = logging.getLogger('mrs')
del logging

# TODO: Switch to using "with" for locks when we stop supporting pre-2.5.
# from __future__ import with_statement


def master_main(registry, user_run, user_setup, args, opts):
    """Run Mrs Master

    Master Main is called directly from Mrs Main.  On exit, the process
    will return master_main's return value.
    """
    from job import Job
    from io import blocking

    # create job thread:
    job = Job(registry, user_run, user_setup, args, opts)
    # create master state:
    master = Master(job, registry, opts)
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
    if not opts.mrs_keep_jobdir:
        from util import remove_recursive
        remove_recursive(job.jobdir)

    # Wait for event thread to finish.
    event_thread.join()

    if master.reaper.traceback:
        logger.critical('Exception: %s' % master.reaper.traceback)


# TODO: when we stop supporting Python older than 2.5, use inlineCallbacks:
class MasterEventThread(TwistedThread):
    """Thread on master that runs the Twisted reactor
    
    We don't trust the Twisted reactor's signal handlers, so we run it with
    the handlers disabled.  As a result, it shouldn't be in the primary
    thread.
    """

    def __init__(self, master):
        TwistedThread.__init__(self)
        self.master = master

    def run(self):
        """Called when the thread is started.
        
        It starts the reactor and schedules signin() to be called.
        """
        reactor.callLater(0, self.master.begin)
        TwistedThread.run(self)

        # Let other threads know that we are quitting.
        self.master.reaper.reap()


class Master(object):
    """Mrs Master"""

    def __init__(self, job, registry, opts, **kwds):
        self.registry = registry
        self.opts = opts

        # TODO: get rid of these:
        self.port = opts.mrs_port
        self.runfile = opts.mrs_runfile
        self.rpc_timeout = opts.mrs_timeout

        self.reaper = GrimReaper()
        self.job = job
        self.job.update_callback = self.job_updated
        self.job.end_callback = self.job_ended

        # stuff that used to be in Supervisor:
        self.assignments = {}
        self.slaves = Slaves()

        self.server_port = None

    # State 1 (run in the event thread):
    def begin(self):
        from twisted.web import server
        from twisted.internet import reactor, error

        # Start RPC master server thread
        resource = MasterInterface(self, self.registry, self.opts)
        site = server.Site(resource)
        try:
            self.server_port = reactor.listenTCP(self.port, site)
        except error.CannotListenError:
            logger.error('Port already in use.')
            self.reaper.reap()
            return
        address = self.server_port.getHost()

        # Report which port we're listening on (and write to a file)
        logger.info('Listening on port %s.' % address.port)
        if self.runfile:
            portfile = open(self.runfile, 'w')
            print >>portfile, address.port
            portfile.close()

    def new_slave(self, host, slave_port, cookie):
        """Create and return a new slave."""
        slave = self.slaves.new_slave(host, slave_port, cookie, self)
        return slave

    def get_slave(self, slave_id, cookie):
        """Get the slave with the given slave_id and cookie."""
        slave = self.slaves.get_slave(slave_id, cookie)
        return slave

    def slave_ready(self, slave):
        """Called when the given slave is ready and idle."""
        # TODO: we might need to resurrect the slave
        if not slave.alive():
            logger.warning('Slave resurrected.')
            slave.resurrect()
        if slave.assignment:
            logger.error('Slave reported ready but still has an assignment.')
            slave.assignment.remove_worker(slave)
        self.slaves.push_idle(slave)

        self.schedule()

    def slave_result(self, slave, urls):
        """Called when the given slave is reporting results."""
        # TODO: what if two slaves finish the same task?  Also, what if
        # one slave finishes and another is still trying?
        assignment = slave.assignment
        assignment.finished(urls)
        self.job.check_done()
        slave.assignment = None

        # It's possible that this was the last thing that needed to be done.
        self.check_quit()

    def slave_gone(self, slave):
        """Called when a slave appears to have died.

        Add the assignment to the todo queue if it is no longer active.
        """
        self.slaves.remove_idle(slave)
        assignment = slave.assignment
        if assignment:
            assignment.remove_worker(slave)

    def schedule(self):
        """Go through the slaves list and make any possible task assignments.
        """
        while True:
            # find the next slave
            slave = self.slaves.pop_idle()
            if slave is None:
                return

            # find the next job to run
            next = self.job.schedule()
            if next is None:
                self.slaves.push_idle(slave)
                return

            assignment = Assignment(next)
            assignment.add_worker(slave)
            deferred = slave.assign(assignment)
            #deferred.addCallback(self.assign_succeed)

    #def assign_succeed(self, value):
    #    """Called when the RPC request completes successfully."""

    def check_quit(self):
        """Triggered when it might be time to quit.

        It's possible that it's time to quit, but it might not be.
        """
        if self.job.done():
            self.quit()

    def quit(self):
        """Start shutting down slaves and self."""
        d = defer.maybeDeferred(self.server_port.stopListening)
        d.addCallback(self.quit2)

    def quit2(self, value):
        """Second stage of quit: disconnect slaves"""

        deferreds = [slave.disconnect() for slave in self.slaves.slave_list()
                if slave.alive()]
        if deferreds:
            dl = defer.DeferredList(deferreds)
            dl.addCallback(self.quit3)
        else:
            self.quit3(None)

    def quit3(self, value):
        """Final stage of quit: kill the reactor"""
        self.reaper.reap()

    ##########################################################################
    # Methods that are called from other threads.

    def job_updated(self):
        """Called when the job is updated--there might be more work to do."""
        reactor.callFromThread(self.schedule)

    def job_ended(self):
        """Called by the job when all datasets have been submitted.

        When the job recognizes that all datasets have been submitted,
        it calls this method from within the job thread.  This does not
        necessarily mean that all computation is complete.

        Called from other threads.
        """
        reactor.callFromThread(self.check_quit)


class MasterInterface(RequestXMLRPC):
    """Public XML-RPC Interface

    Note that any method not beginning with an underscore will be exposed to
    remote hosts.
    """
    def __init__(self, master, registry, opts):
        """Initialize the master's RPC interface.

        Requires `master`, `registry` (a Registry instance which keeps track
        of which names map to which MapReduce functions), and `opts` (which is
        a optparse.Values instance containing command-line arguments on the
        master.
        """
        RequestXMLRPC.__init__(self)
        self.master = master
        self.registry = registry
        self.opts = opts

    @uses_request
    def xmlrpc_whoami(self, request):
        """Return the host of the connecting client.

        The client can't always tell which IP address they're actually using
        from the server's perspective.  This solves that problem.
        """
        host = request.client.host
        return host

    @uses_request
    def xmlrpc_signin(self, request, version, cookie, slave_port, source_hash,
            reg_hash):
        """Slave reporting for duty.

        It returns the slave_id and option dictionary.  Returns (-1, {}) if
        the signin is rejected.
        """
        from version import VERSION

        if version != VERSION:
            logger.warning('Client tried to sign in with mismatched version.')
            return -1, {}
        if not self.registry.verify(source_hash, reg_hash):
            # The slaves are running different code than the master is.
            logger.warning('Client tried to sign in with nonmatching code.')
            return -1, {}
        host = request.client.host
        slave = self.master.new_slave(host, slave_port, cookie)
        if slave is None:
            return -1, {}
        else:
            raw_iter = self.opts.__dict__.iteritems()
            optdict = dict((k, v) for k, v in raw_iter if v is not None)
            return (slave.id, optdict)

    def xmlrpc_ready(self, slave_id, cookie):
        """Slave is ready for work."""
        slave = self.master.get_slave(slave_id, cookie)
        if slave is not None:
            self.master.slave_ready(slave)
            slave.update_timestamp()
            return True
        else:
            logger.error('Slave called ready but id %s not found.' % slave_id)
            return False

    # TODO: The slave should be specific about what it finished.
    def xmlrpc_done(self, slave_id, files, cookie):
        """Slave is done with whatever it was working on.

        The output is available in the list of files.
        """
        slave = self.master.get_slave(slave_id, cookie)
        if slave is not None:
            logger.info('Slave %s reported completion.' % slave_id)
            self.master.slave_result(slave, files)
            self.master.slave_ready(slave)
            slave.update_timestamp()
            return True
        else:
            logger.error('Slave called done but id %s not found.' % slave_id)
            return False

    def xmlrpc_ping(self, slave_id, cookie):
        """Slave checking if we're still here."""
        slave = self.master.get_slave(slave_id, cookie)
        if slave:
            logger.debug('Received a ping from slave %s.' % slave_id)
            slave.update_timestamp()
            return True
        else:
            logger.info('Received a spurious ping.')
            return False


class RemoteSlave(object):
    """The master's view of a remote slave.

    The master can use this object to make assignments, check status, etc.
    """
    def __init__(self, slave_id, host, port, cookie, master):
        self.host = host
        self.port = port
        self.assignment = None
        self.id = slave_id
        self.cookie = cookie
        self.master = master

        from twistrpc import TimeoutProxy
        uri = "http://%s:%s" % (host, port)
        self.rpc = TimeoutProxy(uri, self.master.rpc_timeout)

        self._alive = True
        self.update_timestamp()

        pingdelay = self.master.opts.mrs_pingdelay
        ping_args = ('ping', self.cookie,)
        self.ping_task = PingTask(pingdelay, ping_args, self.rpc,
                self.ping_success, self.rpc_failure, self.get_timestamp)
        self.ping_task.start()

    def check_cookie(self, cookie):
        return (cookie == self.cookie)

    def __hash__(self):
        return hash(self.cookie)

    def assign(self, assignment):
        """Request that the slave start working on the given assignment.

        The request will be made over RPC.  This method returns a deferred.
        """
        from twist import ErrbackException

        task = assignment.task
        extension = task.format.ext
        # TODO: convert these RPC calls to be asynchronous!
        if assignment.map:
            deferred = self.rpc.callRemote('start_map', task.source,
                    task.inurls(), task.map_name, task.part_name,
                    task.splits, task.storage, extension, self.cookie)
        elif assignment.reduce:
            deferred = self.rpc.callRemote('start_reduce', task.source,
                    task.inurls(), task.reduce_name, task.part_name,
                    task.splits, task.storage, extension, self.cookie)
        else:
            raise RuntimeError

        self.assignment = assignment
        deferred.addCallback(self.assign_callback)
        deferred.addErrback(self.rpc_failure)
        return deferred

    def assign_callback(self, value):
        """Called when the start_map or start_reduce call comes back.

        If the value is True, the client accepted the job.  If it's False, the
        client is busy with some other task and rejected the job.
        """
        # FIXME!!!!!!!!!  value could be True or False, depending on
        # whether the slave is already busy
        #print "assign callback", value
        pass

    def update_timestamp(self):
        """Set the timestamp to the current time."""
        from datetime import datetime
        if not self.alive():
            logger.warning('Updating the timestamp of a slave that was dead.')
        self.timestamp = datetime.utcnow()

    def get_timestamp(self):
        """Report the most recent timestamp."""
        return self.timestamp

    def rpc_failure(self, reason=None):
        """Report that a slave failed to respond to an RPC request.

        This may be either a ping or some other request.  At the moment,
        we aren't very lenient, but in the future we could allow a few
        failures before disconnecting the slave.

        Note that we can get multiple failures for one slave.
        """
        if self.alive():
            self.ping_task.stop()
            self._alive = False

            message = 'Lost a slave due to network error'
            if reason:
                message += ': %s' % reason
            else:
                message += '.'
            logger.warning(message)

            # Alert the master:
            self.master.slave_gone(self)

    def ping_success(self):
        """Called when a ping successfully completes."""
        self.update_timestamp()

    def alive(self):
        """Checks whether the Slave is responding."""
        return self._alive

    def resurrect(self):
        self._alive = True
        # Restart the ping_task
        self.ping_task.start()

    def disconnect(self):
        """Disconnect the slave.

        This should be called from the event thread.  It returns a deferred.
        """
        self._alive = False
        self.ping_task.stop()
        deferred = self.rpc.callRemote('quit', self.cookie)
        return deferred


class Slaves(object):
    """List of remote slaves."""
    def __init__(self):
        import threading

        self._slaves = []
        self._idle_slaves = []

    def get_slave(self, slave_id, cookie):
        """Find the slave associated with the given slave_id.
        """
        if slave_id >= len(self._slaves):
            return None
        else:
            slave = self._slaves[slave_id]

        if slave is not None and slave.check_cookie(cookie):
            return slave
        else:
            return None

    def slave_list(self):
        """Get a list of current slaves (_not_ a table keyed by slave_id)."""
        lst = [slave for slave in self._slaves if slave is not None]
        return lst

    def new_slave(self, host, slave_port, cookie, master):
        """Add and return a new slave.

        Also set slave.id for the new slave.  Note that the slave will not be
        added to the idle queue until push_idle is called.
        """
        slave_id = len(self._slaves)
        slave = RemoteSlave(slave_id, host, slave_port, cookie, master)
        self._slaves.append(slave)
        return slave

    def remove_idle(self, slave):
        """Remove a slave from the idle list."""
        if slave in self._idle_slaves:
            self._idle_slaves.remove(slave)

    def push_idle(self, slave):
        """Set a slave as idle.
        """
        if slave.id >= len(self._slaves) or self._slaves[slave.id] is None:
            logger.error('Nonexistent slave pushed to the idle queue.')
        if slave not in self._idle_slaves:
            self._idle_slaves.append(slave)

    def pop_idle(self):
        """Request an idle slave, setting it as busy.

        Return None if all slaves are busy.  Block if requested with the
        blocking parameter.  If you set blocking, we will never return None.
        """
        if self._idle_slaves:
            return self._idle_slaves.pop()


class Assignment(object):
    def __init__(self, task):
        from task import MapTask, ReduceTask
        self.map = isinstance(task, MapTask)
        self.reduce = isinstance(task, ReduceTask)
        self.task = task

        self.done = False
        self.workers = []

    def finished(self, urls):
        self.done = True
        self.task.finished(urls)

    def add_worker(self, slave):
        self.workers.append(slave)
        self.task.active()

    def remove_worker(self, slave):
        try:
            self.workers.remove(slave)
        except ValueError:
            logger.warning('Removed a slave that was not in the worker list.')
        if not self.done and not self.workers:
            self.task.canceled()


# vim: et sw=4 sts=4

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

"""Mrs Master

More information coming soon.
"""

import threading
from mapreduce import Implementation
from twisted.internet import reactor
from twist import TwistedThread, GrimReaper
from twistrpc import RequestXMLRPC, uses_request

# TODO: Switch to using "with" for locks when we stop supporting pre-2.5.
# from __future__ import with_statement


class Parallel(Implementation):
    """MapReduce execution in parallel, with a master and slaves.

    For right now, we require POSIX shared storage (e.g., NFS).
    """
    def __init__(self, job, registry, options, **kwds):
        Implementation.__init__(self, job, registry, options, **kwds)
        self.port = options.mrs_port
        self.runfile = options.mrs_runfile

    def run(self):
        import sys
        from twisted.web import server
        from twisted.internet import reactor
        from twist import TwistedThread, reactor_call

        job = self.job
        job.start()

        master = Master()

        slaves = Slaves()
        tasks = Supervisor(slaves)
        tasks.job = job

        # Start Twisted thread
        event_thread = MasterEventThread(master)
        event_thread.start()

        # Start RPC master server thread
        resource = MasterInterface(slaves, self.registry, self.options)
        site = server.Site(resource)
        tcpport = reactor_call(reactor.listenTCP, self.port, site)
        address = tcpport.getHost()

        print >>sys.stderr, "Listening on port %s" % address.port
        if self.runfile:
            portfile = open(self.runfile, 'w')
            print >>portfile, address.port
            portfile.close()

        # Drive Slaves:
        while not job.done():
            #slaves.activity.wait()
            # work around Python bug where waiting on a Lock can't be
            # interrupted:
            while not slaves.activity.isSet():
                slaves.activity.wait(100000)

            tasks.check_gone()
            tasks.check_done()
            tasks.make_assignments()

        for slave in slaves.slave_list():
            slave.quit()

        # Wait for the other threads to finish.
        event_thread.shutdown()
        event_thread.join()
        job.join()


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
        #reactor.callLater(0, self.master.first_state_function__fixme)
        TwistedThread.run(self)

        # Let other threads know that we are quitting.
        self.master.quit()


class Master(object):
    """Mrs Master"""

    def __init__(self):
        self.reaper = GrimReaper()

    def die(self, exception):
        """Die with an exception."""
        self.reaper.reap(exception)

    def quit(self, message=None):
        """Called to quit the slave."""
        if message:
            import sys
            print >>sys.stderr, message
        self.reaper.reap()


class MasterInterface(RequestXMLRPC):
    """Public XML-RPC Interface

    Note that any method not beginning with an underscore will be exposed to
    remote hosts.
    """
    def __init__(self, slaves, registry, options):
        """Initialize the master's RPC interface.

        Requires `slaves` (an instance of Slaves), `registry` (a Registry
        instance which keeps track of which names map to which MapReduce
        functions), and `options` (which is a optparse.Values instance
        containing command-line arguments on the master.
        """
        RequestXMLRPC.__init__(self)
        self.slaves = slaves
        self.registry = registry
        self.options = options

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
            print "Client tried to sign in with mismatched version."
            return -1, {}
        if not self.registry.verify(source_hash, reg_hash):
            # The slaves are running different code than the master is.
            print "Client tried to sign in with nonmatching code."
            return -1, {}
        host = request.client.host
        slave = self.slaves.new_slave(host, slave_port, cookie)
        if slave is None:
            return -1, {}
        else:
            raw_iter = self.options.__dict__.iteritems()
            optdict = dict((k, v) for k, v in raw_iter if v is not None)
            return (slave.id, optdict)

    def xmlrpc_ready(self, slave_id, cookie):
        """Slave is ready for work."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            self.slaves.push_idle(slave)
            self.slaves.activity.set()
            return True
        else:
            print "In ready(), slave with id %s not found." % slave_id
            return False

    # TODO: The slave should be specific about what it finished.
    def xmlrpc_done(self, slave_id, files, cookie):
        """Slave is done with whatever it was working on.

        The output is available in the list of files.
        """
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            self.slaves.add_done(slave, files)
            slave.update_timestamp()
            return True
        else:
            print "In done(), slave with id %s not found." % slave_id
            return False

    def xmlrpc_ping(self, slave_id, cookie):
        """Slave checking if we're still here."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave:
            slave.update_timestamp()
            return True
        else:
            return False


class RemoteSlave(object):
    """The master's view of a remote slave.

    The master can use this object to make assignments, check status, etc.
    """
    def __init__(self, slave_id, host, port, cookie, activity):
        self.host = host
        self.port = port
        self.assignment = None
        self.id = slave_id
        self.cookie = cookie

        # An event that is set if activity happens in any of the slaves.
        self.activity = activity

        from twistrpc import MrsRPCProxy
        uri = "http://%s:%s" % (host, port)
        self.rpc = MrsRPCProxy(uri)

        self._alive = True
        self.update_timestamp()

        from twist import PingTask
        self.ping_task = PingTask(self)
        self.ping_task.start()

    def check_cookie(self, cookie):
        return (cookie == self.cookie)

    def __hash__(self):
        return hash(self.cookie)

    def assign(self, assignment):
        """Request that the slave start working on the given assignment.

        The request will be made over RPC.
        """
        from twist import ErrbackException

        task = assignment.task
        extension = task.format.ext
        # TODO: convert these RPC calls to be asynchronous!
        try:
            if assignment.map:
                self.rpc.blocking_call('start_map', task.taskid,
                        task.inurls(), task.map_name, task.part_name,
                        task.nparts, task.outdir, extension, self.cookie)
            elif assignment.reduce:
                self.rpc.blocking_call('start_reduce', task.taskid,
                        task.inurls(), task.reduce_name, task.part_name,
                        task.nparts, task.outdir, extension, self.cookie)
            else:
                raise RuntimeError
        except ErrbackException:
            self.rpc_failure()
        self.assignment = assignment

    def update_timestamp(self):
        """Set the timestamp to the current time."""
        from datetime import datetime
        if not self.alive():
            import sys
            print >>sys.stderr, ("Warning: updating the timestamp of a slave"
                    " we thought was dead!")
        self.timestamp = datetime.utcnow()

    def timestamp_since(self, other):
        """Report whether the timestamp is newer than the given time."""
        return self.timestamp > other

    def rpc_failure(self):
        """Report that a slave failed to respond to an RPC request.

        This may be either a ping or some other request.  At the moment,
        we aren't very lenient, but in the future we could allow a few
        failures before disconnecting the slave.
        """
        self.ping_task.stop()
        self._alive = False

        print 'Lost slave due to network error.'

        # Alert the main thread that activity has occurred.
        self.activity.set()

    def alive(self):
        """Checks whether the Slave is responding."""
        return self._alive

    def quit(self):
        self._alive = False
        self.ping_task.stop()
        try:
            self.rpc.blocking_call('quit', self.cookie)
        except ErrbackException:
            pass


# TODO: Reimplement _idle_sem as a Condition variable.  Also, reimplement
# _done_slaves as a shared queue.
class Slaves(object):
    """List of remote slaves.

    A Slaves list is shared by the master thread and the rpc server thread.
    """
    def __init__(self):
        import threading
        self.activity = threading.Event()

        self._slaves = []
        self._idle_slaves = []
        self._done_slaves = []

        self._lock = threading.Lock()
        self._idle_sem = threading.Semaphore()

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
        self._lock.acquire()
        lst = [slave for slave in self._slaves if slave is not None]
        self._lock.release()
        return lst

    def new_slave(self, host, slave_port, cookie):
        """Add and return a new slave.

        Also set slave.id for the new slave.  Note that the slave will not be
        added to the idle queue until push_idle is called.
        """
        self._lock.acquire()
        slave_id = len(self._slaves)
        slave = RemoteSlave(slave_id, host, slave_port, cookie, self.activity)
        self._slaves.append(slave)
        self._lock.release()
        return slave

    def remove_slave(self, slave):
        """Remove a slave, whether it is busy or idle.

        Presumably, the slave has stopped responding.
        """
        # TODO: Should we allow the slave to report in again later if it
        # really is still alive?
        self._lock.acquire()
        if slave in self._idle_slaves:
            # Note that we don't decrement the semaphore.  Tough luck for the
            # sap that thinks the list has more entries than it does.
            self._idle_slaves.remove(slave)
        self._slaves[slave.id] = None
        self._lock.release()

    def push_idle(self, slave):
        """Set a slave as idle.
        """
        self._lock.acquire()
        if slave.id >= len(self._slaves) or self._slaves[slave.id] is None:
            self._lock.release()
            import sys
            print >>sys.stderr, ("Nonexistent slave can't be pushed to "
                    "the idle queue!")
        if slave not in self._idle_slaves:
            self._idle_slaves.append(slave)
        self._idle_sem.release()
        self._lock.release()

    def pop_idle(self, blocking=False):
        """Request an idle slave, setting it as busy.

        Return None if all slaves are busy.  Block if requested with the
        blocking parameter.  If you set blocking, we will never return None.
        """
        idler = None
        while idler is None:
            if self._idle_sem.acquire(blocking):
                self._lock.acquire()
                try:
                    idler = self._idle_slaves.pop()
                except IndexError:
                    # This can happen if remove_slave was called.  So sad.
                    pass
                self._lock.release()
            if not blocking:
                break
        return idler

    def add_done(self, slave, files):
        self._lock.acquire()
        self._done_slaves.append((slave, files))
        self._lock.release()

        # Alert the main thread that activity has occurred.
        self.activity.set()

    def pop_done(self):
        self._lock.acquire()
        if self._done_slaves:
            done = self._done_slaves.pop()
        else:
            done = None
        self._lock.release()
        return done


class Assignment(object):
    def __init__(self, task):
        from task import MapTask, ReduceTask
        self.map = isinstance(task, MapTask)
        self.reduce = isinstance(task, ReduceTask)
        self.task = task

        self.done = False
        self.workers = []
    
    def finished(self, urls):
        self.task.finished(urls)

    def add_worker(self, slave):
        self.workers.append(slave)
        self.task.active()

    def remove_worker(self, slave):
        try:
            self.workers.remove(slave)
        except ValueError:
            print "Slave wasn't in the worker list.  Is this a problem?"
        if not self.workers:
            self.task.canceled()


class Supervisor(object):
    """Keep track of tasks and workers.

    Initialize with a Slaves object.
    """
    def __init__(self, slaves):
        self.job = None
        self.assignments = {}
        self.slaves = slaves

    def assign(self, slave):
        """Assign a task to the given slave.

        Return the assignment, if made, or None if there are no available
        tasks.
        """
        if slave.assignment is not None:
            raise RuntimeError
        next = self.job.schedule()
        if next is not None:
            assignment = Assignment(next)
            slave.assign(assignment)
            assignment.add_worker(slave)
            return assignment

    def remove_slave(self, slave):
        """Remove a slave that may be currently working on a task.

        Add the assignment to the todo queue if it is no longer active.
        """
        self.slaves.remove_slave(slave)
        assignment = slave.assignment
        if not assignment:
            return
        assignment.remove_worker(slave)

    # TODO: what if two slaves finish the same task?
    def check_done(self):
        """Check for slaves that have completed their assignments."""
        while True:
            next_done = self.slaves.pop_done()
            if next_done is None:
                return
            slave, urls = next_done

            assignment = slave.assignment
            assignment.finished(urls)
            self.job.check_done()

            slave.assignment = None
            self.slaves.push_idle(slave)

    def check_gone(self):
        """Check for slaves that have disappeared."""
        for slave in self.slaves.slave_list():
            if not slave.alive():
                self.remove_slave(slave)

    def make_assignments(self):
        """Go through the slaves list and make any possible task assignments.
        """
        while True:
            idler = self.slaves.pop_idle()
            if idler is None:
                return
            assignment = self.assign(idler)
            if assignment is None:
                self.slaves.push_idle(idler)
                return

# vim: et sw=4 sts=4

# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
# more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Mrs.  If not, see <http://www.gnu.org/licenses/>.

MAIN_LOOP_WAIT = 2.0
#SOCKET_TIMEOUT = 5.0
SOCKET_TIMEOUT = 10.0
PING_LOOP_WAIT = 1.0

import socket, threading
from mapreduce import Implementation

# NOTE: This is a _global_ setting:
socket.setdefaulttimeout(SOCKET_TIMEOUT)


class Parallel(Implementation):
    """MapReduce execution in parallel, with a master and slaves.

    For right now, we require POSIX shared storage (e.g., NFS).
    """
    def __init__(self, job, registry, options, **kwds):
        Implementation.__init__(self, job, registry, options, **kwds)
        self.port = options.mrs_port

    def run(self):
        import sys
        import master, rpc
        from twisted.web import server
        from twisted.internet import reactor
        from twist import TwistedThread, reactor_call

        job = self.job
        job.start()

        slaves = master.Slaves()
        tasks = Supervisor(slaves)
        tasks.job = job

        # Start Twisted thread
        twisted_thread = TwistedThread()
        twisted_thread.start()

        # Start RPC master server thread
        resource = master.MasterInterface(slaves, self.registry, self.options)
        site = server.Site(resource)
        tcpport = reactor_call(reactor.listenTCP, self.port, site)
        address = tcpport.getHost()

        print >>sys.stderr, "Listening on port %s" % address.port

        # Drive Slaves:
        while not job.done():
            slaves.activity.wait()

            tasks.check_gone()
            tasks.check_done()
            tasks.make_assignments()

        for slave in slaves.slave_list():
            slave.quit()

        # Wait for the other thread to finish.
        job.join()


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

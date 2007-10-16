#!/usr/bin/env python

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

WAIT_LIMIT = 2.0
SOCKET_TIMEOUT = 1.0

import socket
from mapreduce import Job, MapTask, ReduceTask, interm_dir, interm_file

# NOTE: This is a _global_ setting:
socket.setdefaulttimeout(SOCKET_TIMEOUT)


def run_master(mapper, reducer, partition, inputs, output, options):
    """Mrs Master
    """
    map_tasks = options.map_tasks
    reduce_tasks = options.reduce_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1

    if map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    from mrs.mapreduce import Operation
    op = Operation(mapper, reducer, partition, map_tasks=map_tasks,
            reduce_tasks=reduce_tasks)
    mrsjob = ParallelJob(inputs, output, options.port, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0

def run_slave(mapper, reducer, partition, uri, options):
    """Mrs Slave

    The uri is of the form scheme://username:password@host/target with
    username and password possibly omitted.
    """
    import slave, rpc
    import select, xmlrpclib

    # Create an RPC proxy to the master's RPC Server
    cookie = slave.rand_cookie()
    master = xmlrpclib.ServerProxy(uri)

    # Start up a worker thread.  This thread will die when we do.
    worker = slave.Worker(master, cookie, mapper, reducer, partition)
    worker.start()

    # Startup a slave RPC Server
    slave_rpc = slave.SlaveRPC(cookie, worker)
    server = rpc.new_server(slave_rpc, options.port)
    server_fd = server.fileno()
    host, port = server.socket.getsockname()

    # Register with master.
    if not master.signin(slave_rpc.cookie, port):
        import sys
        print >>sys.stderr, "Master rejected signin."
        return -1

    while slave_rpc.alive:
        rlist, wlist, xlist = select.select([server_fd], [], [], WAIT_LIMIT)
        if server_fd in rlist:
            server.handle_request()
        else:
            # try to ping master
            try:
                master_alive = master.ping()
            except:
                master_alive = False
            if not master_alive:
                import sys
                print >>sys.stderr, "Master failed to respond to ping."
                return -1
    return 0

def try_makedirs(path):
    import os
    try:
        os.makedirs(path)
    except OSError, e:
        import errno
        if e.errno != errno.EEXIST:
            raise


class ParallelJob(Job):
    """MapReduce execution in parallel, with a master and slaves.

    For right now, we require POSIX shared storage (e.g., NFS).
    """
    def __init__(self, inputs, outdir, port, shared_dir, **kwds):
        Job.__init__(self, **kwds)
        self.inputs = inputs
        self.outdir = outdir
        self.port = port
        self.shared_dir = shared_dir

    def run(self):
        ################################################################
        # TEMPORARY LIMITATIONS
        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        op = self.operations[0]

        map_tasks = op.map_tasks
        if map_tasks != len(self.inputs):
            raise NotImplementedError("Requires exactly 1 map_task per input.")

        reduce_tasks = op.reduce_tasks
        ################################################################

        import sys, os
        import formats, master, rpc
        from tempfile import mkstemp, mkdtemp
        from datetime import datetime

        slaves = master.Slaves()
        tasks = Supervisor(slaves)

        # Start RPC master server thread
        master_rpc = master.MasterRPC(slaves)
        rpc_thread = rpc.RPCThread(master_rpc, self.port)
        rpc_thread.start()
        port = rpc_thread.server.socket.getsockname()[1]
        print >>sys.stderr, "Listening on port %s" % port

        # Prep:
        try_makedirs(self.outdir)
        try_makedirs(self.shared_dir)
        jobdir = mkdtemp(prefix='mrs.job_', dir=self.shared_dir)
        for i in xrange(reduce_tasks):
            os.mkdir(interm_dir(jobdir, i))

        # Create Map Tasks:
        for taskid, filename in enumerate(self.inputs):
            map_task = MapTask(taskid, op.mapper, op.partition, filename,
                    jobdir, reduce_tasks)
            tasks.push_todo(Assignment(map_task))

        # Create Reduce Tasks:
        for taskid in xrange(op.reduce_tasks):
            reduce_task = ReduceTask(taskid, op.reducer, self.outdir, jobdir)
            tasks.push_todo(Assignment(reduce_task))

        # Drive Slaves:
        while not tasks.job_complete():
            slaves.activity.wait(WAIT_LIMIT)
            slaves.activity.clear()

            now = datetime.utcnow()

            # TODO: check for done slaves!
            # slaves.pop_done()

            tasks.check_done()
            tasks.make_assignments()

            for slave in slaves.slave_list():
                # Ping the next slave:
                if not slave.alive(now):
                    print >>sys.stderr, "Slave not responding."
                    tasks.remove_slave(slave)

                # Try to make all new assignments:
                tasks.make_assignments()

        for slave in slaves.slave_list():
            slave.quit()


class Assignment(object):
    def __init__(self, task):
        self.map = isinstance(task, MapTask)
        self.reduce = isinstance(task, ReduceTask)
        self.task = task

        self.done = False
        self.workers = []

    def __cmp__(self, other):
        if self.map and other.reduce:
            return -1
        elif self.reduce and other.map:
            return 1
        else:
            # both map or both reduce: make this more complex later:
            return 0


class Supervisor(object):
    """Keep track of tasks and workers.

    Initialize with a Slaves object.
    """
    def __init__(self, slaves):
        self.todo = []
        self.active = []
        self.completed = []

        self.assignments = {}
        self.slaves = slaves

        # For now, you can't start a reduce task until all maps are done:
        self.map_tasks_remaining = 0

    def push_todo(self, assignment):
        """Add a new assignment that needs to be completed."""
        from heapq import heappush
        heappush(self.todo, assignment)
        if assignment.map:
            self.map_tasks_remaining += 1

    def pop_todo(self):
        """Pop the next available assignment."""
        from heapq import heappop
        if self.todo and (self.todo[0].map or self.map_tasks_remaining == 0):
            return heappop(self.todo)
        else:
            return None

    def set_active(self, assignment):
        """Move an assignment from the todo queue and to the active list."""
        from heapq import heappush
        self.active.append(assignment)

    def assign(self, slave):
        """Assign a task to the given slave.

        Return the assignment, if made, or None if there are no available
        tasks.
        """
        if slave.assignment is not None:
            raise RuntimeError
        next = self.pop_todo()
        if next is not None:
            slave.assign(next)
            next.workers.append(slave)
            self.set_active(next)
        return next

    def remove_slave(self, slave):
        """Remove a slave that may be currently working on a task.

        Add the assignment to the todo queue if it is no longer active.
        """
        self.slaves.remove_slave(slave)
        assignment = slave.assignment
        if not assignment:
            return
        assignment.workers.remove(slave)
        if not assignment.workers:
            self.active.remove(assignment)
            self.push_todo(assignment)

    def check_done(self):
        """Check for slaves that have completed their assignments.
        """
        while True:
            slave = self.slaves.pop_done()
            if slave is None:
                return

            print "Active Tasks:", len(self.active)
            print "Map Tasks Left:", self.map_tasks_remaining
            print "Todo Tasks:", len(self.todo)

            assignment = slave.assignment
            if assignment.map:
                self.map_tasks_remaining -= 1
            self.active.remove(assignment)
            self.completed.append(assignment)

            slave.assignment = None
            self.slaves.push_idle(slave)

    def make_assignments(self):
        """Go through the slaves list and make any possible task assignments.
        """
        while True:
            idler = self.slaves.pop_idle()
            if idler is None:
                return
            assignment = self.assign(idler)
            if assignment is None:
                return

    def job_complete(self):
        if self.todo or self.active:
            return False
        else:
            return True


# vim: et sw=4 sts=4

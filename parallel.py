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

MAIN_LOOP_WAIT = 2.0
SOCKET_TIMEOUT = 5.0
PING_LOOP_WAIT = 1.0

import socket, threading
from mapreduce import Job, MapTask, ReduceTask
from util import try_makedirs

# NOTE: This is a _global_ setting:
socket.setdefaulttimeout(SOCKET_TIMEOUT)


def run_master(mrs_prog, inputs, output, options):
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
    op = Operation(mrs_prog, map_tasks=map_tasks, reduce_tasks=reduce_tasks)
    mrsjob = ParallelJob(inputs, output, options.port, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


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
        import master, rpc
        from tempfile import mkstemp, mkdtemp

        slaves = master.Slaves()
        tasks = Supervisor(slaves)

        # Start RPC master server thread
        interface = master.MasterInterface(slaves)
        rpc_thread = rpc.RPCThread(interface, self.port)
        rpc_thread.start()
        port = rpc_thread.server.socket.getsockname()[1]
        print >>sys.stderr, "Listening on port %s" % port

        # Start pinging thread
        ping_thread = PingThread(slaves)
        ping_thread.start()

        # Prep:
        try_makedirs(self.outdir)
        try_makedirs(self.shared_dir)
        jobdir = mkdtemp(prefix='mrs.job_', dir=self.shared_dir)

        # Create Map Tasks:
        map_stage = Stage()
        for taskid, filename in enumerate(self.inputs):
            map_task = MapTask(taskid, op.mrs_prog, filename, jobdir,
                    reduce_tasks)
            map_stage.push_todo(Assignment(map_task))
        tasks.stages.append(map_stage)

        # Create Reduce Tasks:
        reduce_stage = Stage()
        for taskid in xrange(op.reduce_tasks):
            reduce_task = ReduceTask(taskid, op.mrs_prog, self.outdir, jobdir)
            reduce_stage.push_todo(Assignment(reduce_task))
        tasks.stages.append(reduce_stage)

        map_stage.dependents.append(reduce_stage)

        # Drive Slaves:
        while not tasks.job_complete():
            slaves.activity.wait(MAIN_LOOP_WAIT)
            slaves.activity.clear()

            tasks.check_gone()
            tasks.check_done()
            tasks.make_assignments()
            tasks.print_status()

        for slave in slaves.slave_list():
            slave.quit()


class PingThread(threading.Thread):
    """Occasionally ping slaves that need a little extra attention."""

    def __init__(self, slaves, **kwds):
        threading.Thread.__init__(self, **kwds)
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)
        self.slaves = slaves

    @classmethod
    def seconds(cls, delta):
        return ((delta.days * 24 * 3600) + delta.seconds +
                (delta.microseconds / 1000000.0))

    def run(self):
        from datetime import datetime
        import time
        now = datetime.utcnow()

        while True:
            last = now
            for slave in self.slaves.slave_list():
                if not slave.alive(now):
                    import sys
                    print >>sys.stderr, "Slave not responding."
                    self.slaves.add_gone(slave)
                    self.slaves.activity.set()
            now = datetime.utcnow()
            delta = self.seconds(now - last)
            if delta < PING_LOOP_WAIT:
                time.sleep(PING_LOOP_WAIT - delta)

class Assignment(object):
    def __init__(self, task):
        self.map = isinstance(task, MapTask)
        self.reduce = isinstance(task, ReduceTask)
        self.task = task

        self.done = False
        self.workers = []


class Stage(object):
    """Mrs Stage (Map Stage or Reduce Stage)

    The stage describes the dependency structure for a whole set of tasks.
    """
    # TODO: allow stages to be a dependency graph instead of a queue.
    def __init__(self):
        self.todo = []
        self.active = []
        self.done = []
        self.dependents = []

    def push_todo(self, assignment):
        """Add a new assignment that needs to be completed."""
        from heapq import heappush
        heappush(self.todo, assignment)

    def pop_todo(self):
        """Pop the next available assignment."""
        if self.todo:
            from heapq import heappop
            return heappop(self.todo)
        else:
            return None

    def add_active(self, assignment):
        """Add an assignment to the active list."""
        self.active.append(assignment)

    def print_status(self):
        active = len(self.active)
        todo = len(self.todo)
        done = len(self.done)
        total = active + todo + done
        print 'Current Stage.  Active: %s; Complete: %s/%s' % (active, done,
                total)

class Supervisor(object):
    """Keep track of tasks and workers.

    Initialize with a Slaves object.
    """
    def __init__(self, slaves):
        self.stages = []
        self.completed = []

        self.assignments = {}
        self.slaves = slaves

    def print_status(self):
        if self.stages:
            self.stages[0].print_status()

    def check_stages(self):
        """See if any stages have finished so a new one can start running.

        For now, we assume that only one stage can run at a time, and that
        each stage is dependent on all previous ones.
        """
        current = self.stages[0]
        if not (current.todo or current.active):
            self.stages.remove(current)
            self.completed.append(current)
            for dep in current.dependents:
                for i, consumer in enumerate(dep.todo):
                    for provider in current.done:
                        interm_file = provider.files[i]
                        if interm_file:
                            consumer.task.inputs.append(interm_file)

    def assign(self, slave):
        """Assign a task to the given slave.

        Return the assignment, if made, or None if there are no available
        tasks.
        """
        if slave.assignment is not None:
            raise RuntimeError
        if self.stages:
            current_stage = self.stages[0]
            next = current_stage.pop_todo()
        else:
            next = None
        if next is not None:
            slave.assign(next)
            next.workers.append(slave)
            current_stage.add_active(next)
        return next

    def remove_slave(self, slave):
        """Remove a slave that may be currently working on a task.

        Add the assignment to the todo queue if it is no longer active.
        """
        self.slaves.remove_slave(slave)
        assignment = slave.assignment
        if not assignment:
            return
        try:
            assignment.workers.remove(slave)
        except ValueError:
            print "Slave wasn't in the worker list.  Is this a problem?"
        if not assignment.workers:
            current_stage = self.stages[0]
            current_stage.active.remove(assignment)
            current_stage.push_todo(assignment)

    # TODO: what if two slaves finish the same task?
    def check_done(self):
        """Check for slaves that have completed their assignments.
        """
        while True:
            next_done = self.slaves.pop_done()
            if next_done is None:
                return
            slave, files = next_done
            current_stage = self.stages[0]

            assignment = slave.assignment
            assignment.files = files
            current_stage.active.remove(assignment)
            current_stage.done.append(assignment)

            slave.assignment = None
            self.slaves.push_idle(slave)
            self.check_stages()

    def check_gone(self):
        """Check for slaves that have disappeared.
        """
        while True:
            slave = self.slaves.pop_gone()
            if slave is None:
                return
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

    def job_complete(self):
        if self.stages:
            return False
        else:
            return True


# vim: et sw=4 sts=4

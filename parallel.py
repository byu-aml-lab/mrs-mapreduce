#!/usr/bin/env python

MAIN_LOOP_WAIT = 2.0
SOCKET_TIMEOUT = 5.0
PING_LOOP_WAIT = 1.0

import socket, threading
from mapreduce import Job, Implementation, MapTask, ReduceTask
from util import try_makedirs

# NOTE: This is a _global_ setting:
socket.setdefaulttimeout(SOCKET_TIMEOUT)


def run_master(registry, run, inputs, output, options):
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
    op = Operation(registry, run, map_tasks=map_tasks,
            reduce_tasks=reduce_tasks)
    mrsjob = Parallel(inputs, output, options.port, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


class Parallel(Implementation):
    """MapReduce execution in parallel, with a master and slaves.

    For right now, we require POSIX shared storage (e.g., NFS).
    """
    def __init__(self, inputs, outdir, port, shared_dir, **kwds):
        Implementation.__init__(self, **kwds)
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
        interface = master.MasterInterface(slaves, op.registry)
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

        job = Job()
        map_out = job.map_data(self.inputs, op.registry, 'mapper',
                'partition', len(self.inputs), reduce_tasks)
        reduce_out = job.reduce_data(map_out, op.registry, 'reducer',
                'partition', reduce_tasks, 1)
        tasks.job = job

        #for taskid, filename in enumerate(self.inputs):
            #map_task = MapTask(taskid, op.registry, 'mapper', 'partition',
            #        jobdir, reduce_tasks)
            #map_task.inputs = [filename]
        #for taskid in xrange(op.reduce_tasks):
            #reduce_task = ReduceTask(taskid, op.registry, 'reducer',
            #        self.outdir, jobdir)

        # Drive Slaves:
        while not job.done():
            slaves.activity.wait(MAIN_LOOP_WAIT)
            slaves.activity.clear()

            tasks.check_gone()
            tasks.check_done()
            tasks.make_assignments()
            tasks.job.print_status()

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
    
    def finished(self):
        self.task.finished()

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
        next = self.job.get_task()
        if next is not None:
            slave.assign(next)
            next.add_worker(slave)
        return next

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
        """Check for slaves that have completed their assignments.
        """
        while True:
            next_done = self.slaves.pop_done()
            if next_done is None:
                return
            slave, files = next_done

            assignment = slave.assignment
            assignment.files = files
            assignment.finished()

            slave.assignment = None
            self.slaves.push_idle(slave)

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


# vim: et sw=4 sts=4

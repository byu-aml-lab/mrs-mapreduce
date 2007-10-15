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

PING_INTERVAL = 5.0
SOCKET_TIMEOUT = 1.0

import socket
from mapreduce import Job, MapTask

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
        rlist, wlist, xlist = select.select([server_fd], [], [], PING_INTERVAL)
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


class ParallelJob(Job):
    """MapReduce execution in parallel, with a master and slaves.

    For right now, we require POSIX shared storage (e.g., NFS).
    """
    def __init__(self, inputs, output_dir, port, shared_dir, **kwds):
        Job.__init__(self, **kwds)
        self.inputs = inputs
        self.output_dir = output_dir
        self.port = port
        self.shared_dir = shared_dir

    # TODO: break this function into several smaller ones:
    def run(self):
        ################################################################
        # TEMPORARY LIMITATIONS
        if len(self.operations) != 1:
            raise NotImplementedError("Requires exactly one operation.")
        operation = self.operations[0]

        map_tasks = operation.map_tasks
        if map_tasks != len(self.inputs):
            raise NotImplementedError("Requires exactly 1 map_task per input.")

        reduce_tasks = operation.reduce_tasks
        ################################################################

        import sys, os
        import formats, master, rpc
        from tempfile import mkstemp, mkdtemp

        # Start RPC master server thread
        master_rpc = master.MasterRPC()
        rpc_thread = rpc.RPCThread(master_rpc, self.port)
        rpc_thread.start()
        port = rpc_thread.server.socket.getsockname()[1]
        print >>sys.stderr, "Listening on port %s" % port

        # Prep:
        jobdir = mkdtemp(prefix='mrs.job_', dir=self.shared_dir)

        interm_path = os.path.join(jobdir, 'interm_')
        interm_dirs = [interm_path + str(i) for i in xrange(reduce_tasks)]
        for name in interm_dirs:
            os.mkdir(name)

        output_dir = os.path.join(jobdir, 'output')
        os.mkdir(output_dir)


        from heapq import heapify, heappop, heappush
        assignments = {}
        tasks = []

        # TODO: Make tasks a simple list instead of a heap (?)

        # Create Map Tasks:
        for taskid, filename in enumerate(self.inputs):
            map_task = MapTask(taskid, operation.mapper, operation.partition,
                    filename, interm_path, reduce_tasks)
            heappush(tasks, map_task)
        

        # Create Reduce Tasks:
        # PLEASE WRITE ME


        # Drive Slaves:
        while True:
            master_rpc.activity.wait(PING_INTERVAL)
            master_rpc.activity.clear()

            self.make_assignments(master_rpc, tasks, assignments)

            for slave in master_rpc.slaves.slave_list():
                # Ping the next slave:
                try:
                    slave_alive = slave.slave_rpc.ping()
                except:
                    slave_alive = False
                if not slave_alive:
                    print >>sys.stderr, "Slave failed to respond to ping."
                    master_rpc.slaves.remove_slave(slave)
                    assignment = assignments.get(slave)
                    if assignment:
                        del assignments[slave]
                        tasks[assignment.taskid].remove(slave)
                    # resort:
                    heapify(tasks)

                # Try to make all new assignments:
                self.make_assignments(master_rpc, tasks, assignments)


    def make_assignments(self, master_rpc, tasks, assignments):
        from heapq import heappop, heappush
        while True:
            idler = master_rpc.slaves.pop_idle()
            if idler is None:
                break
            if idler.done:
                # FINISH THIS PART!!!
                task = idler.task
                idler.done = False
            newtask = heappop(tasks)
            idler.assign_task(newtask)
            assignments[idler.cookie] = newtask
            # Repush with the new number of workers
            heappush(tasks, newtask)


        ### IN PROGRESS ###

            ######################

#        for reducer_id in xrange(operation.reduce_tasks):
#            # SORT PHASE
#            interm_directory = interm_path + str(reducer_id)
#            fd, sorted_name = mkstemp(prefix='mrs.sorted_')
#            os.close(fd)
#            interm_filenames = [os.path.join(interm_directory, s)
#                    for s in os.listdir(interm_directory)]
#            formats.hexfile_sort(interm_filenames, sorted_name)
#
#            # REDUCE PHASE
#            sorted_file = formats.HexFile(open(sorted_name))
#            basename = 'reducer_%s' % reducer_id
#            output_name = os.path.join(self.output_dir, basename)
#            output_file = operation.output_format(open(output_name, 'w'))
#
#            reduce(operation.reducer, sorted_file, output_file)
#
#            sorted_file.close()
#            output_file.close()


# vim: et sw=4 sts=4

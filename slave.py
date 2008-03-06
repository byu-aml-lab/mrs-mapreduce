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

COOKIE_LEN = 8
SLAVE_PING_INTERVAL = 5.0

import threading

class SlaveInterface(object):
    """Public XML-RPC Interface
    
    Note that any method not beginning with an underscore will be exposed to
    remote hosts.
    """
    def __init__(self, slave):
        self.slave = slave

    def _listMethods(self):
        return SimpleXMLRPCServer.list_public_methods(self)

    def start_map(self, taskid, inputs, func_name, part_name, nparts, output,
            extension, cookie, **kwds):
        self.slave.check_cookie(cookie)
        return self.slave.worker.start_map(taskid, inputs, func_name,
                part_name, nparts, output, extension)

    def start_reduce(self, taskid, inputs, func_name, part_name, nparts,
            output, extension, cookie, **kwds):
        return self.slave.worker.start_reduce(taskid, inputs, func_name,
                part_name, nparts, output, extension)

    def quit(self, cookie, **kwds):
        self.slave.check_cookie(cookie)
        self.slave.alive = False
        import sys
        print >>sys.stderr, "Quitting as requested by an RPC call."
        return True

    def ping(self, **kwds):
        """Master checking if we're still here.
        """
        return True


class CookieValidationError(Exception):
    pass


def run_slave(registry, user_run, user_setup, args, opts):
    """Mrs Slave

    The uri is of the form scheme://username:password@host/target with
    username and password possibly omitted.
    """
    import xmlrpclib

    # Create an RPC proxy to the master's RPC Server
    master = xmlrpclib.ServerProxy(opts.mrs_master)
    slave = Slave(master, registry, user_setup, opts.mrs_port)

    slave.run()
    return 0


class Slave(object):
    def __init__(self, master, registry, user_setup, port):
        import rpc

        self.master = master
        self.registry = registry
        self.user_setup = user_setup
        self.port = port

        self.id = None
        self.alive = True
        self.cookie = self.rand_cookie()

        # Create a worker thread.  This thread will die when we do.
        self.worker = Worker(self, master)

        # Create a slave RPC Server
        self.interface = SlaveInterface(self)
        self.server = rpc.new_server(self.interface, port)

        self.host, self.port = self.server.socket.getsockname()

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

    def handle_request(self):
        """Try to handle a request on the RPC connection.

        Timeout after SLAVE_PING_INTERVAL seconds.
        """
        import select
        server_fd = self.server.fileno()
        rlist, wlist, xlist = select.select([server_fd], [], [],
                SLAVE_PING_INTERVAL)
        if server_fd in rlist:
            self.server.handle_request()
            return True
        else:
            return False

    def run(self):
        import socket, optparse
        from version import VERSION

        # Register with master.
        source_hash = self.registry.source_hash()
        reg_hash = self.registry.reg_hash()
        slave_id, optdict = self.master.signin(VERSION, self.cookie,
                self.port, source_hash, reg_hash)
        if slave_id < 0:
            import sys
            print >>sys.stderr, "Master rejected signin."
            raise RuntimeError
        self.id = slave_id
        options = optparse.Values(optdict)

        # Call the user_setup function with the given options.
        if self.user_setup:
            self.user_setup(options)

        # Spin off the worker thread.
        self.worker.start()

        # Report for duty.
        self.master.ready(self.id, self.cookie)

        # Handle requests on the RPC server.
        while self.alive:
            if not self.handle_request():
                try:
                    master_alive = self.master.ping(self.id, self.cookie)
                except socket.error:
                    master_alive = False
                if not master_alive:
                    import sys
                    print >>sys.stderr, "Master failed to respond to ping."
                    return -1


class Worker(threading.Thread):
    """Execute map tasks and reduce tasks.

    The worker waits for other threads to make assignments by calling
    start_map and start_reduce.
    """
    def __init__(self, slave, master, **kwds):
        threading.Thread.__init__(self, **kwds)
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

        self.slave = slave
        self.master = master

        self._task = None
        self._cond = threading.Condition()
        self.active = False
        self.exception = None

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
        """Run the worker thread."""

        # We put this here because reactor.run() can only be called once.  We
        # cheat by putting this here.  In the future, it would probably be
        # best to build more of Mrs around the event loop.
        from twisted.internet import reactor
        reactor.startRunning(installSignalHandlers=0)

        while True:
            self._cond.acquire()
            while self._task is None:
                self._cond.wait()
            task = self._task
            self._cond.release()

            task.run()

            self._cond.acquire()
            self._task = None
            self._cond.release()

            # TODO: right now, dataset.dump() happens in task.run().  Instead,
            # we should tell the dataset to become available() here, and the
            # data should automatically be dumped.
            self.master.done(self.slave.id, task.outurls(), self.slave.cookie)


# vim: et sw=4 sts=4

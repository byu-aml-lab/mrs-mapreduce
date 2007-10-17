#!/usr/bin/env python

# TODO: Switch to using "with" for locks when we stop supporting pre-2.5.
# from __future__ import with_statement

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

MASTER_PING_INTERVAL = 5.0


class MasterInterface(object):
    """Public XML-RPC Interface

    Note that any method not beginning with an underscore will be exposed to
    remote hosts.
    """
    def __init__(self, slaves):
        self.slaves = slaves

    def _listMethods(self):
        import SimpleXMLRPCServer
        return SimpleXMLRPCServer.list_public_methods(self)

    def signin(self, cookie, slave_port, host=None, port=None):
        """Slave reporting for duty.

        Returns -1 if the signin is rejected.
        """
        # TODO: make the slave id be the entry in the slaves list.
        next_id = self.slaves.next_id()
        slave = RemoteSlave(next_id, host, slave_port, cookie)
        self.slaves.add_slave(slave)
        return next_id

    def ready(self, slave_id, cookie, **kwds):
        """Slave is ready for work."""
        slave = self.slaves.get_slave(cookie)
        self.slaves.push_idle(slave)
        self.slaves.activity.set()
        return True

    def done(self, cookie, **kwds):
        """Slave is done with whatever it was working on.
        """
        slave = self.slaves.get_slave(cookie)
        self.slaves.add_done(slave)
        slave.update_timestamp()
        return True

    # TODO: Find out which slave is pinging us and update_timestamp().
    def ping(self, **kwds):
        """Slave checking if we're still here.
        """
        # TODO: return False if they're not signed in
        return True


class RemoteSlave(object):
    def __init__(self, slave_id, host, port, cookie):
        self.host = host
        self.port = port
        self.assignment = None
        self.id = slave_id
        self.cookie = cookie

        import xmlrpclib
        uri = "http://%s:%s" % (host, port)
        self.slave_rpc = xmlrpclib.ServerProxy(uri)

        self.update_timestamp()

    def __hash__(self):
        return hash(self.cookie)

    def assign(self, assignment):
        task = assignment.task
        if assignment.map:
            self.slave_rpc.start_map(task.taskid, task.input, task.jobdir,
                    task.reduce_tasks, self.cookie)
        elif assignment.reduce:
            self.slave_rpc.start_reduce(task.taskid, task.outdir, task.jobdir,
                    self.cookie)
        else:
            raise RuntimeError
        self.assignment = assignment

    def update_timestamp(self):
        from datetime import datetime
        self.timestamp = datetime.utcnow()

    def alive(self, now=None):
        """Checks whether the Slave has been checked on recently.

        Note that MASTER_PING_INTERVAL defines "recently."  We will ping the
        slave if we haven't heard from them in that amount of time.  If now is
        given (as a result from datetime.datetime.utcnow()), use it to avoid
        having to check too often.
        """
        import datetime
        ping_delta = datetime.timedelta(seconds=MASTER_PING_INTERVAL)
        if now is None:
            now = datetime.datetime.utcnow()
        delta = now - self.timestamp
        if delta < ping_delta:
            return True
        else:
            try:
                alive = self.slave_rpc.ping()
            except:
                alive = False
            if alive:
                self.update_timestamp()
            return alive

    def quit(self):
        self.slave_rpc.quit(self.cookie)


# TODO: Reimplement _idle_sem as a Condition variable.
class Slaves(object):
    def __init__(self):
        import threading
        self.activity = threading.Event()

        self._slaves = {}
        self._idle_slaves = []
        self._done_slaves = []
        self._next_id = 0

        self._lock = threading.Lock()
        self._idle_sem = threading.Semaphore()

    def next_id(self):
        """Get the next slave id."""
        # TODO: make the slave id be the entry in the slaves list.
        self._lock.acquire()
        value = self._next_id
        self._next_id += 1
        self._lock.release()
        return value

    def get_slave(self, cookie, host=None):
        """Find the slave associated with the given cookie.
        """
        return self._slaves[cookie]

    def slave_list(self):
        """Get a snapshot of the current slaves.
        """
        self._lock.acquire()
        lst = self._slaves.values()
        self._lock.release()
        return lst

    def add_slave(self, slave):
        """Add a new slave.

        It will not be added to the idle queue until push_idle is called.
        """
        self._lock.acquire()
        self._slaves[slave.cookie] = slave
        self._lock.release()

    def remove_slave(self, slave):
        """Remove a slave, whether it is busy or idle.

        Presumably, the slave has stopped responding.
        """
        self._lock.acquire()
        if slave in self._idle_slaves:
            # Note that we don't decrement the semaphore.  Tough luck for the
            # sap that thinks the list has more entries than it does.
            self._idle_slaves.remove(slave)
        del self._slaves[slave.cookie]
        self._lock.release()

    def push_idle(self, slave):
        """Set a slave as idle.
        """
        self._lock.acquire()
        if slave.cookie not in self._slaves:
            self._lock.release()
            raise RuntimeError("Slave does not exist!")
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

    def add_done(self, slave):
        self._lock.acquire()
        self._done_slaves.append(slave)
        self._lock.release()

        self.activity.set()

    def pop_done(self):
        self._lock.acquire()
        try:
            done_slave = self._done_slaves.pop()
        except IndexError:
            done_slave = None
        self._lock.release()
        return done_slave


if __name__ == '__main__':
    # Testing standalone server.
    import rpc
    instance = MasterInterface()
    PORT = 8000
    #PORT = 0
    server = rpc.new_server(instance, host='127.0.0.1', port=PORT)
    server.serve_forever()


# vim: et sw=4 sts=4

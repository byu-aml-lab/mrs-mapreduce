#!/usr/bin/env python

# TODO: Switch to using "with" for locks when we stop supporting pre-2.5.
# from __future__ import with_statement

MASTER_PING_INTERVAL = 5.0


class MasterInterface(object):
    """Public XML-RPC Interface

    Note that any method not beginning with an underscore will be exposed to
    remote hosts.
    """
    def __init__(self, slaves, registry):
        self.slaves = slaves
        self.registry = registry

    def _listMethods(self):
        import SimpleXMLRPCServer
        return SimpleXMLRPCServer.list_public_methods(self)

    def whoami(self, host=None, port=None):
        """Return the host of the connecting client.

        The client can't always tell which IP address they're actually using
        from the server's perspective.  This solves that problem.
        """
        return host

    def signin(self, cookie, slave_port, source_hash, reg_hash, host=None,
            port=None):
        """Slave reporting for duty.

        Returns -1 if the signin is rejected.
        """
        if not self.registry.verify(source_hash, reg_hash):
            # The slaves are running different code than the master is.
            return -1
        slave = self.slaves.new_slave(host, slave_port, cookie)
        if slave is None:
            return -1
        else:
            return slave.id

    def ready(self, slave_id, cookie, **kwds):
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
    def done(self, slave_id, files, cookie, **kwds):
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

    def ping(self, slave_id, cookie, **kwds):
        """Slave checking if we're still here.
        """
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

    def check_cookie(self, cookie):
        return (cookie == self.cookie)

    def __hash__(self):
        return hash(self.cookie)

    def assign(self, assignment):
        """Request that the slave start working on the given assignment.

        The request will be made over RPC.
        """
        task = assignment.task
        if assignment.map:
            self.slave_rpc.start_map(task.map_name, task.part_name,
                    task.taskid, task.inurls(), task.outdir, task.nparts,
                    self.cookie)
        elif assignment.reduce:
            self.slave_rpc.start_reduce(task.reduce_name, task.taskid,
                    task.inputurls(), task.outdir, self.cookie)
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
            #timestamp = datetime.datetime.utcnow()
            try:
                alive = self.slave_rpc.ping()
            except Exception, e:
                print 'Ping failed with exception:', e
                alive = False
            #elapsed = datetime.datetime.utcnow() - timestamp
            #print 'Elapsed time for ping (alive=%s):' % alive, elapsed
            if alive:
                self.update_timestamp()
            return alive

    def quit(self):
        self.slave_rpc.quit(self.cookie)


# TODO: Reimplement _idle_sem as a Condition variable.  Also, reimplement
# _done_slaves and _gone_slaves as shared queues.
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
        self._gone_slaves = []

        self._lock = threading.Lock()
        self._idle_sem = threading.Semaphore()

    def get_slave(self, slave_id, cookie):
        """Find the slave associated with the given slave_id.
        """
        if slave_id >= len(self._slaves):
            return None
        else:
            slave = self._slaves[slave_id]

        if slave.check_cookie(cookie):
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
        slave = RemoteSlave(slave_id, host, slave_port, cookie)
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

    def add_done(self, slave, files):
        self._lock.acquire()
        self._done_slaves.append((slave, files))
        self._lock.release()

        self.activity.set()

    def pop_done(self):
        self._lock.acquire()
        if self._done_slaves:
            done = self._done_slaves.pop()
        else:
            done = None
        self._lock.release()
        return done

    def add_gone(self, slave):
        self._lock.acquire()
        self._gone_slaves.append(slave)
        self._lock.release()

        self.activity.set()

    def pop_gone(self):
        self._lock.acquire()
        if self._gone_slaves:
            slave = self._gone_slaves.pop()
        else:
            slave = None
        self._lock.release()
        return slave


if __name__ == '__main__':
    # Testing standalone server.
    import rpc
    instance = MasterInterface(None, None)
    PORT = 8080
    server = rpc.new_server(instance, host='127.0.0.1', port=PORT)
    server.serve_forever()


# vim: et sw=4 sts=4

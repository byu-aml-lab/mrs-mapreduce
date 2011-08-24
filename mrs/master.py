# Mrs
# Copyright 2008-2011 Brigham Young University
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

# Note that the actual backlog may be limited by the OS--in Linux see:
# /proc/sys/net/core/somaxconn (which seems to be 128 by default)

import os
import socket
import sys
import threading
import time
import xmlrpclib

from . import datasets
from . import http
from . import pool
from . import registry
from . import runner
from . import task

import logging
logger = logging.getLogger('mrs')
del logging


class MasterRunner(runner.TaskRunner):
    def __init__(self, *args):
        super(MasterRunner, self).__init__(*args)

        self.slaves = None
        self.idle_slaves = set()
        self.dead_slaves = set()
        self.current_assignments = set()
        self.result_storage = {}

        self.rpc_interface = None
        self.rpc_thread = None
        self.sched_pipe = None

        self.runqueue = None
        self.runqueue_pipe = None

    def run(self):
        self.runqueue_pipe, runqueue_write_pipe = os.pipe()
        self.register_fd(self.runqueue_pipe, self.read_runqueue_pipe)
        self.runqueue = pool.RunQueue(runqueue_write_pipe)
        threadpool = pool.ThreadPool(self.runqueue)
        threadpool_thread = threading.Thread(target=threadpool.run,
                name='Thread Pool')
        threadpool_thread.daemon = True
        threadpool_thread.start()

        self.sched_pipe, sched_write_pipe = os.pipe()
        self.register_fd(self.sched_pipe, self.read_sched_pipe)
        self.slaves = Slaves(sched_write_pipe, self.runqueue,
                self.opts.mrs__timeout, self.opts.mrs__pingdelay)

        try:
            self.start_rpc_server()
            self.eventloop(timeout_function=self.maintain_runqueue)
        finally:
            self.slaves.disconnect_all()

    def start_rpc_server(self):
        program_hash = registry.object_hash(self.program_class)
        self.rpc_interface = MasterInterface(self.slaves, program_hash,
                self.opts, self.args, self.jobdir)
        port = getattr(self.opts, 'mrs__port', 0)
        rpc_server = http.ThreadingRPCServer(('', port), self.rpc_interface)
        if port == 0:
            port = rpc_server.server_address[1]

        self.rpc_thread = threading.Thread(target=rpc_server.serve_forever,
                name='RPC Server')
        self.rpc_thread.daemon = True
        self.rpc_thread.start()

        logger.info('Listening on port %s.' % port)
        if self.opts.mrs__runfile:
            with open(self.opts.mrs__runfile, 'w') as f:
                print >>f, port

    def read_runqueue_pipe(self):
        """Reads currently available data from runqueue_pipe.

        The actual data is ignored--the pipe is just a mechanism for
        interrupting the select loop if it's blocking.
        """
        os.read(self.runqueue_pipe, 4096)

    def maintain_runqueue(self):
        """Maintains the runqueue and returns the timeout value for poll."""
        timeleft = self.runqueue.time_to_reschedule()
        if (timeleft is not None) and (timeleft <= 0):
            self.runqueue.reschedule()
            timeleft = self.runqueue.time_to_reschedule()
        return timeleft

    def read_sched_pipe(self):
        """Reads currently available data from sched_pipe.

        The actual data is ignored--the pipe is just a mechanism for alerting
        the runner that the schedule method needs to be called.
        """
        os.read(self.sched_pipe, 4096)
        self.schedule()

    def schedule(self):
        """Check for any changed slaves and make task assignments."""
        for slave, dataset_id, source, urls in self.slaves.get_results():
            if dataset_id in self.result_storage:
                self.result_storage[dataset_id].append((slave, source))

            if (dataset_id, source) in self.current_assignments:
                self.task_done(dataset_id, source, urls)
                self.current_assignments.remove((dataset_id, source))
            else:
                logger.info('Ignoring a redundant result (%s, %s).' %
                        (dataset_id, source))

        for slave in self.slaves.get_changed_slaves():
            if slave.alive():
                self.dead_slaves.discard(slave)
                if slave.idle():
                    self.idle_slaves.add(slave)
            else:
                self.dead_slaves.add(slave)
                self.idle_slaves.discard(slave)
                assignment = slave.pop_assignment()
                if assignment is not None:
                    self.ready_tasks.appendleft(assignment)

        while self.idle_slaves:
            slave = self.idle_slaves.pop()
            # find the next job to run
            next = self.next_task()
            if next is None:
                # TODO: duplicate currently assigned tasks here (while adding
                # a mechanism for unassigning duplicate tasks once the result
                # comes back).
                self.idle_slaves.add(slave)
                break

            if not slave.idle():
                logger.error('Slave %s mistakenly in idle_slaves.' % slave.id)
                continue

            dataset_id, source = next
            if dataset_id not in self.result_storage:
                self.result_storage[dataset_id] = []
            slave.assign(next, self.datasets)
            self.current_assignments.add(next)

    def remove_dataset(self, ds):
        if isinstance(ds, datasets.ComputedData):
            delete = not ds.permanent
            for slave, source in self.result_storage[ds.id]:
                self.runqueue.do(slave.remove, args=(ds.id, source, delete))
            del self.result_storage[ds.id]
        super(MasterRunner, self).remove_dataset(ds)

    def debug_status(self):
        super(MasterRunner, self).debug_status()
        print >>sys.stderr, 'Current assignments:', (
                ', '.join('(%s, %s)' % a for a in self.current_assignments))
        print >>sys.stderr, 'Idle slaves:', (
                ', '.join(str(slave.id) for slave in self.idle_slaves))
        print >>sys.stderr, 'Dead slaves:', (
                ', '.join(str(slave.id) for slave in self.dead_slaves))


class MasterInterface(object):
    """Public XML-RPC Interface

    Note that any method beginning with "xmlrpc_" will be exposed to
    remote hosts.
    """
    def __init__(self, slaves, program_hash, opts, args, jobdir):
        """Initialize the master's RPC interface.

        Requires `master`, `program_hash` (the result of registry.object_hash
        on the program class), and `opts` (which is a optparse.Values instance
        containing command-line arguments on the master.
        """
        self.slaves = slaves
        self.program_hash = program_hash
        self.opts = opts
        self.args = args
        self.jobdir = jobdir

    def xmlrpc_whoami(self, request):
        """Return the host of the connecting client.

        The client can't always tell which IP address they're actually using
        from the server's perspective.  This solves that problem.
        """
        host = request.client.host
        return host

    @http.uses_host
    def xmlrpc_signin(self, version, cookie, slave_port, program_hash,
            host=None):
        """Slave reporting for duty.

        It returns the slave_id and option dictionary.  Returns (-1, {}, [])
        if the signin is rejected.
        """
        from version import VERSION

        if version != VERSION:
            logger.warning('Client tried to sign in with mismatched version.')
            return -1, '', '', {}, []
        if self.program_hash != program_hash:
            # The slaves are running different code than the master is.
            logger.warning('Client tried to sign in with nonmatching code.')
            return -1,'', '', {}, []

        slave = self.slaves.new_slave(host, slave_port, cookie)
        logger.info('New slave %s on host %s' % (slave.id, host))

        raw_iter = vars(self.opts).iteritems()
        optdict = dict((k, v) for k, v in raw_iter if v is not None)
        return (slave.id, host, self.jobdir, optdict, self.args)

    @http.uses_host
    def xmlrpc_ready(self, slave_id, cookie, host=None):
        """Slave is ready for work."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.info('Slave %s ready.' % slave_id)
            slave.update_timestamp()
            self.slaves.slave_ready(slave)
            return True
        else:
            logger.error('Invalid slave reported ready (host %s, id %s).'
                    % (host, slave_id))
            return False

    @http.uses_host
    def xmlrpc_done(self, slave_id, dataset_id, source, urls, cookie,
            host=None):
        """Slave is done with whatever it was working on.

        The output is available in the list of urls.
        """
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.info('Slave %s reported completion of task: %s, %s'
                    % (slave_id, dataset_id, source))
            slave.update_timestamp()
            self.slaves.slave_result(slave, dataset_id, source, urls)
            return True
        else:
            logger.error('Invalid slave reported done (host %s, id %s).'
                    % (host, slave_id))
            return False

    @http.uses_host
    def xmlrpc_ping(self, slave_id, cookie, host=None):
        """Slave checking if we're still here."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.debug('Received a ping from slave %s.' % slave_id)
            slave.update_timestamp()
            return True
        else:
            logger.error('Invalid slave sent ping (host %s, id %s).'
                    % (host, slave_id))
            return False


class RemoteSlave(object):
    """The master's view of a remote slave.

    The master can use this object to make assignments, check status, etc.
    """
    def __init__(self, slave_id, host, port, cookie, slaves):
        self.id = slave_id
        self.host = host
        self.port = port
        self.cookie = cookie
        self.slaves = slaves
        self.runqueue = slaves.runqueue
        self.pingdelay = slaves.pingdelay

        uri = "http://%s:%s" % (host, port)
        self._rpc = http.ServerProxy(uri, slaves.rpc_timeout)
        self._rpc_lock = threading.Lock()

        self._assignment = None
        self._assignment_lock = threading.Lock()
        self._rpc_func = None
        self._rpc_args = None

        # The `_state` is either 'alive', 'failed', 'exiting', or 'exited'
        self._state = 'alive'

        # The ping_repeating variable assures that only one ping "task" is
        # active at a time.
        self._ping_repeating = False
        self._pinglock = threading.Lock()
        self._schedule_ping(self.pingdelay)
        self.update_timestamp()

    def check_cookie(self, cookie):
        return (cookie == self.cookie)

    def idle(self):
        """Indicates whether the slave can take a new assignment."""
        return (self._assignment == None)

    def pop_assignment(self, assignment=None):
        """Removes and returns the current assignment."""
        with self._assignment_lock:
            assignment = self._assignment
            self._assignment = None
            return assignment

    def set_assignment(self, old_assignment, new_assignment):
        """Sets the assignment to new_assignment if the old_assignment matches.
        """
        with self._assignment_lock:
            if self._assignment == old_assignment:
                self._assignment = new_assignment
                return True
            else:
                return False

    def assign(self, assignment, datasets):
        """Schedules an RPC request to make the slave work on the assignment.

        Called from the Runner.
        """
        assert self.set_assignment(None, assignment)
        dataset_id, source = assignment

        dataset = datasets[dataset_id]
        if dataset.task_class == task.MapTask:
            self._rpc_func = self._rpc.start_map
        elif dataset.task_class == task.ReduceTask:
            self._rpc_func = self._rpc.start_reduce
        else:
            assert False, 'Unknown task class: %s' % repr(dataset.task_class)

        input_ds = datasets[dataset.input_id]
        #urls = [(b.source, b.url) for b in input_ds[:, source] if b.url]
        urls = [b.url for b in input_ds[:, source] if b.url]

        func_name = dataset.func_name
        part_name = dataset.part_name
        logger.info('Assigning task to slave %s: %s, %s'
                % (self.id, dataset_id, source))
        storage = dataset.dir if dataset.dir else ''
        if dataset.format is not None:
            ext = dataset.format.ext
        else:
            ext = ''

        self._rpc_args = (dataset_id, source, urls, func_name, part_name,
                dataset.splits, storage, ext, self.cookie)

        self.runqueue.do(self.send_assignment)

    def send_assignment(self):
        with self._rpc_lock:
            if not self.alive():
                logger.warning('Canceling RPC call because slave %s is no'
                        ' longer alive.' % self.id)
                return

            try:
                success = self._rpc_func(*self._rpc_args)
            except xmlrpclib.Fault, f:
                logger.error('Fault in RPC call to slave %s: %s'
                        % (self.id, f.faultString))
                success = False
            except xmlrpclib.ProtocolError, e:
                logger.error('Protocol error in RPC call to slave %s: %s'
                        % (self.id, e.errmsg))
                success = False
            except socket.timeout:
                logger.error('Timeout in RPC call to slave %s' % self.id)
                success = False
            except socket.error, e:
                logger.error('Socket error in RPC call to slave %s: %s'
                        % (self.id, e.args[1]))
                success = False

            if success:
                self.update_timestamp()

        self._rpc_func = None
        self._rpc_args = None

        if not success:
            logger.info('Failed to assign a task to slave %s.' % self.id)
            self.critical_failure()

    def remove(self, dataset_id, source, delete):
        with self._rpc_lock:
            logger.info('Sending remove request to slave %s: %s, %s'
                    % (self.id, dataset_id, source))
            if not self.alive():
                # Note: the master may disconnect the slave while remove
                # requests are still pending--this isn't really a bad thing.
                return

            try:
                self._rpc.remove(dataset_id, source, delete, self.cookie)
                success = True
            except xmlrpclib.Fault, f:
                logger.error('Fault in remove call to slave %s: %s'
                        % (self.id, f.faultString))
                success = False
            except xmlrpclib.ProtocolError, e:
                logger.error('Protocol error in remove call to slave %s: %s'
                        % (self.id, e.errmsg))
                success = False
            except socket.timeout:
                logger.error('Timeout in remove call to slave %s' % self.id)
                success = False
            except socket.error, e:
                logger.error('Socket error in remove call to slave %s: %s'
                        % (self.id, e.args[1]))
                success = False

            if success:
                self.update_timestamp()

        if not success:
            logger.info('Failed to remove data on slave %s.' % self.id)
            self.critical_failure()

    def update_timestamp(self):
        """Set the timestamp to the current time."""
        if self._state in ('exiting', 'exited'):
            return
        if self._state == 'failed':
            logger.warning('Updating timestamp of the failed slave %s.'
                    % self.id)
            self.resurrect()
        self.timestamp = time.time()

    def critical_failure(self):
        """Report that a slave had a critical failure.

        Note that we can get multiple failures for one slave.
        """
        if self._state == 'alive':
            self._state = 'failed'

            logger.error('Lost slave %s (%s).' % (self.id, self.host))
            self.slaves.slave_dead(self)

    def alive(self):
        """Checks whether the Slave is responding."""
        return self._state == 'alive'

    def exited(self):
        """Checks whether the Slave has been given an exit request."""
        return self._state == 'exited'

    def resurrect(self):
        if self._state == 'failed':
            logger.warning('Slave %s (%s): resurrected' % (self.id, self.host))
            with self._pinglock:
                self._state = 'alive'
                self._schedule_ping(self.pingdelay)
            return True
        else:
            return False

    def ping(self):
        """Ping the slave and schedule a follow-up ping."""
        with self._pinglock:
            if not self.alive():
                self._ping_repeating = False
                return

        delta = time.time() - self.timestamp
        if delta < self.pingdelay:
            self._schedule_ping(self.pingdelay - delta, from_ping_method=True)
            return

        if not self._rpc_lock.acquire(False):
            # RPC socket busy; try again soon.
            self.runqueue.do(self.ping)
            self._schedule_ping(from_ping_method=True)
            return

        try:
            self._rpc.ping(self.cookie)
            success = True
        except xmlrpclib.Fault, f:
            logger.error('Fault in ping to slave %s: %s'
                    % (self.id, f.faultString))
            success = False
        except xmlrpclib.ProtocolError, e:
            logger.error('Protocol error in ping to slave %s: %s'
                    % (self.id, e.errmsg))
            success = False
        except socket.timeout:
            logger.error('Timeout in ping to slave %s' % self.id)
            success = False
        except socket.error, e:
            logger.error('Socket error in ping to slave %s: %s'
                    % (self.id, e.args[1]))
            success = False
        finally:
            self._rpc_lock.release()

        if success:
            self.update_timestamp()
            self._schedule_ping(self.pingdelay, from_ping_method=True)
        else:
            self.critical_failure()

    def _schedule_ping(self, delay=None, from_ping_method=False):
        """Schedules a ping to occur after the given delay.

        Ensures that the ping keeps repeating, i.e., when a ping finishes,
        a new ping is immediately scheduled.
        """
        if not from_ping_method:
            if self._ping_repeating:
                return
            else:
                self._ping_repeating = True
        self.runqueue.do(self.ping, delay=delay)

    def disconnect(self):
        """Disconnect the slave by sending a quit request."""
        if self._state not in ('exiting', 'exited'):
            self._state = 'exiting'
            self.runqueue.do(self.send_quit)

    def send_quit(self):
        with self._rpc_lock:
            try:
                logger.info('Sending a quit request to slave %s' % self.id)
                self._rpc.quit(self.cookie)
            except xmlrpclib.Fault, f:
                logger.error('Fault in quit to slave %s: %s'
                        % (self.id, f.faultString))
            except xmlrpclib.ProtocolError, e:
                logger.error('Protocol error in quit to slave %s: %s'
                        % (self.id, e.errmsg))
            except socket.timeout:
                logger.error('Timeout in quit to slave %s' % self.id)
            except socket.error, e:
                logger.error('Socket error in quit to slave %s: %s'
                        % (self.id, e.args[1]))
            self._state = 'exited'
            self._rpc = None


class Slaves(object):
    """List of remote slaves."""
    def __init__(self, sched_pipe, runqueue, rpc_timeout, pingdelay):
        self._sched_pipe = sched_pipe
        self.runqueue = runqueue
        self.rpc_timeout = rpc_timeout
        self.pingdelay = pingdelay

        self._lock = threading.Lock()
        self._next_slave_id = 0
        self._slaves = {}

        self._changed_slaves = set()
        self._results = list()

    def trigger_sched(self):
        """Wakes up the runner for scheduling by sending it a byte."""
        os.write(self._sched_pipe, ' ')

    def get_slave(self, slave_id, cookie):
        """Find the slave associated with the given slave_id."""
        with self._lock:
            slave = self._slaves.get(slave_id, None)

        if slave is not None and slave.check_cookie(cookie):
            return slave
        else:
            return None

    def new_slave(self, host, slave_port, cookie):
        """Add and return a new slave.

        Also set slave.id for the new slave.  Note that the slave will not be
        added to the idle queue until push_idle is called.
        """
        with self._lock:
            slave_id = self._next_slave_id
            self._next_slave_id += 1
            slave = RemoteSlave(slave_id, host, slave_port, cookie, self)
            self._slaves[slave_id] = slave
        return slave

    def slave_ready(self, slave):
        if not slave.alive():
            if not slave.resurrect():
                return

        with self._lock:
            if not slave.idle():
                logger.error('Slave %s reported ready but is not idle; '
                        'check the slave logs for errors.' % slave.id)
            self._changed_slaves.add(slave)

        self.trigger_sched()

    def slave_result(self, slave, dataset_id, source, urls):
        with self._lock:
            self._results.append((slave, dataset_id, source, urls))
            self._changed_slaves.add(slave)
        assert slave.set_assignment((dataset_id, source), None)
        self.trigger_sched()

    def slave_dead(self, slave):
        with self._lock:
            self._changed_slaves.add(slave)
        self.trigger_sched()

    def get_results(self):
        """Return and reset the list of results: (taskid, urls) pairs."""
        with self._lock:
            results = self._results
            self._results = []
        return results

    def get_changed_slaves(self):
        """Return and reset the list of changed slaves."""
        with self._lock:
            changed = self._changed_slaves
            self._changed_slaves = set()
        return changed

    def disconnect_all(self):
        for slave in self._slaves.itervalues():
            slave.disconnect()


# vim: et sw=4 sts=4

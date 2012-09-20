# Mrs
# Copyright 2008-2012 Brigham Young University
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

"""Mrs Master"""

from __future__ import division, print_function

# Note that the actual backlog may be limited by the OS--in Linux see:
# /proc/sys/net/core/somaxconn (which seems to be 128 by default)

import collections
import os
import socket
import sys
import threading
import time

from . import http
from . import registry
from . import computed_data
from . import runner
from . import tasks
from .version import __version__


try:
    from xmlrpc.client import Fault, ProtocolError
except ImportError:
    from xmlrpclib import Fault, ProtocolError

import logging
logger = logging.getLogger('mrs')
del logging

# Python 3 compatibility
PY3 = sys.version_info[0] == 3
if not PY3:
    range = xrange


INITIAL_PEON_THREADS = 4
MAX_PEON_THREADS = 20


class MasterRunner(runner.TaskRunner):
    """A TaskRunner that assigns tasks to remote slaves.

    Attributes:
        idle_slaves: a set of slaves that are ready to be assigned
        result_maps: a dict mapping a dataset id to the corresponding result
            map, which keeps track of which slaves produced which data
    """
    def __init__(self, *args):
        super(MasterRunner, self).__init__(*args)

        self.slaves = None
        self.idle_slaves = IdleSlaves()
        self.dead_slaves = set()
        self.current_assignments = set()
        self.result_maps = {}

        self.rpc_interface = None
        self.rpc_thread = None
        self.sched_pipe = None

    def run(self):
        for i in range(INITIAL_PEON_THREADS):
            self.start_peon_thread()
        self.sched_timing_stats()

        self.sched_pipe, sched_write_pipe = os.pipe()
        self.event_loop.register_fd(self.sched_pipe, self.read_sched_pipe)
        self.slaves = Slaves(sched_write_pipe, self.chore_queue,
                self.opts.mrs__timeout, self.opts.mrs__pingdelay)

        try:
            self.start_rpc_server()
            self.event_loop.run(timeout_function=self.maintain_chore_queue)
        finally:
            if self.opts.mrs__runfile:
                # Rewrite the runfile with a hyphen to signify that
                # execution is complete.
                with open(self.opts.mrs__runfile, 'w') as f:
                    print('-', file=f)
            self.slaves.disconnect_all()
        return self.exitcode

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
                print(port, file=f)

    def maintain_chore_queue(self):
        """Maintains the chore_queue and returns the timeout value for poll."""
        timeleft = self.chore_queue.time_to_reschedule()
        if (timeleft is not None) and (timeleft <= 0):
            self.chore_queue.reschedule()
            timeleft = self.chore_queue.time_to_reschedule()
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
        for dataset_id, task_index in self.slaves.get_failed_tasks():
            self.task_lost(dataset_id, task_index)

        changed_slaves = self.slaves.get_changed_slaves()
        results = self.slaves.get_results()

        for slave in changed_slaves:
            if slave.alive():
                self.dead_slaves.discard(slave)
                if not slave.busy():
                    logger.debug('Adding slave %s to idle_slaves.' % slave.id)
                    self.idle_slaves.add(slave)
            else:
                self.dead_slaves.add(slave)
                self.idle_slaves.discard(slave)
                assignment = slave.current_assignment()
                if assignment is not None:
                    dataset_id, task_index = assignment
                    self.task_lost(dataset_id, task_index)

        for slave, dataset_id, source, urls in results:
            try:
                self.result_maps[dataset_id].add(slave, source)
            except KeyError:
                # Dataset already deleted, so this source should be removed.
                self.remove_sources(dataset_id, (slave, source), delete=True)

            if (dataset_id, source) in self.current_assignments:
                # Note: if this is the last task in the dataset, this will wake
                # up datasets.  Thus this happens _after_ slaves are added to
                # the idle_slaves set.
                self.task_done(dataset_id, source, urls)
                self.current_assignments.remove((dataset_id, source))
            else:
                logger.info('Ignoring a redundant result (%s, %s).' %
                        (dataset_id, source))

        # Add one peon thread for each new active slave (minus dead slaves).
        if self.peon_thread_count < MAX_PEON_THREADS:
            slave_count = len(self.slaves) - len(self.dead_slaves)
            new_peon_thread_count = slave_count + INITIAL_PEON_THREADS
            for i in range(new_peon_thread_count - self.peon_thread_count):
                self.start_peon_thread()

        chore_list = []
        while self.idle_slaves:
            # find the next job to run
            next = self.next_task()
            if next is None:
                # TODO: duplicate currently assigned tasks here (while adding
                # a mechanism for unassigning duplicate tasks once the result
                # comes back).
                break
            dataset_id, source = next
            dataset = self.datasets[dataset_id]

            slave = None
            if dataset.affinity:
                # Slave-task affinity: when possible, assign to the slave that
                # computed the task with the same source id in the input
                # dataset.
                input_id = dataset.input_id
                try:
                    input_results = self.result_maps[input_id]
                except KeyError:
                    input_results = None

                if input_results is not None:
                    for s in input_results.get(source):
                        if s in self.idle_slaves:
                            self.idle_slaves.remove(s)
                            slave = s
                            break
            if slave is None:
                slave = self.idle_slaves.pop()

            if slave.busy():
                logger.error('Slave %s mistakenly in idle_slaves.' % slave.id)
                self.task_lost(*next)
                continue

            slave.prepare_assignment(next, self.datasets)
            chore_item = slave.send_assignment, ()
            chore_list.append(chore_item)
            self.current_assignments.add(next)
        self.chore_queue.do_many(chore_list)

    def available_workers(self):
        """Returns the total number of idle workers."""
        return len(self.idle_slaves)

    def make_tasklist(self, dataset):
        tasklist = super(MasterRunner, self).make_tasklist(dataset)
        self.result_maps[dataset.id] = ResultMap()
        return tasklist

    def remove_dataset(self, ds):
        if isinstance(ds, computed_data.ComputedData):
            delete = not ds.permanent
            slave_source_list = self.result_maps[ds.id].all()
            self.remove_sources(ds.id, slave_source_list, delete)
            del self.result_maps[ds.id]
        super(MasterRunner, self).remove_dataset(ds)

    def remove_sources(self, dataset_id, slave_source_list, delete):
        """Remove a single source from a slave.

        The `slave_source_list` parameter is a list of slave-source pairs.
        The `delete` parameter specifies whether the file should actually be
        removed from disk.
        """
        items = [(slave.remove, (dataset_id, source, delete))
                for slave, source in slave_source_list]
        self.chore_queue.do_many(items)

    def sched_timing_stats(self):
        if self.opts.mrs__timing_interval > 0:
            self.chore_queue.do(self.do_timing_stats,
                    delay=self.opts.mrs__timing_interval)

    def do_timing_stats(self):
        self.timing_stats()
        self.sched_timing_stats()

    def debug_status(self):
        super(MasterRunner, self).debug_status()
        print('Current assignments:', (', '.join('(%s, %s)' % a
                for a in self.current_assignments)), file=sys.stderr)
        print('Idle slaves:', (', '.join(str(slave.id)
                for slave in self.idle_slaves)), file=sys.stderr)
        print('Dead slaves:', (', '.join(str(slave.id)
                for slave in self.dead_slaves)), file=sys.stderr)


class ResultMap(object):
    """Track which slaves produced which sources in a single dataset."""
    def __init__(self):
        self._dict = collections.defaultdict(list)

    def add(self, slave, source):
        """Records that the given slave computed the given source."""
        self._dict[source].append(slave)

    def get(self, source):
        """Returns the list of slaves that computed the given source."""
        return self._dict[source]

    def all(self):
        """Iterate over slave, source pairs."""
        for source, slave_list in self._dict.items():
            for slave in slave_list:
                yield slave, source


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
        opts_iter = vars(opts).items()
        self.opts_dict = dict((k, v) for k, v in opts_iter if v is not None)
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

        It returns the slave_id and option dictionary.  Returns
        (-1, '', '', {}, []) if the signin is rejected.
        """
        if version != __version__:
            logger.warning('Slave tried to sign in with mismatched version.')
            return -1, '', '', {}, []
        if self.program_hash != program_hash:
            # The slaves are running different code than the master is.
            logger.warning('Slave tried to sign in with nonmatching code.')
            return -1, '', '', {}, []

        slave = self.slaves.new_slave(host, slave_port, cookie)
        if slave is None:
            logger.warning('Slave tried to sign in during shutdown.')
            return -1, '', '', {}, []
        logger.info('New slave %s on host %s' % (slave.id, host))

        return (slave.id, host, self.jobdir, self.opts_dict, self.args)

    @http.uses_host
    def xmlrpc_ready(self, slave_id, cookie, host=None):
        """Slave is ready for work."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.debug('Slave %s ready.' % slave_id)
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
        """Slave is done with the task it was working on.

        The output is available in the list of urls.
        """
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.debug('Slave %s reported completion of task: %s, %s'
                    % (slave_id, dataset_id, source))
            slave.update_timestamp()
            self.slaves.slave_result(slave, dataset_id, source, urls)
            return True
        else:
            logger.error('Invalid slave reported done (host %s, id %s).'
                    % (host, slave_id))
            return False

    @http.uses_host
    def xmlrpc_failed(self, slave_id, dataset_id, task_index, cookie,
            host=None):
        """Slave failed to complete the task it was working on."""
        slave = self.slaves.get_slave(slave_id, cookie)
        if slave is not None:
            logger.error('Slave %s reported failure of task: %s, %s'
                    % (slave_id, dataset_id, task_index))
            slave.update_timestamp()
            self.slaves.slave_failed(slave, dataset_id, task_index)
            return True
        else:
            logger.error('Invalid slave reported failed (host %s, id %s).'
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
        self.chore_queue = slaves.chore_queue
        self.pingdelay = slaves.pingdelay

        uri = "http://%s:%s" % (host, port)
        self._rpc = http.TimeoutServerProxy(uri, slaves.rpc_timeout)
        self._rpc_lock = threading.Lock()

        self._assignment = None
        self._assignment_lock = threading.Lock()
        self._rpc_func = None
        self._rpc_args = None

        # The `_state` is either 'alive', 'failed', 'exiting', or 'exited'
        self._state = 'alive'

        # The pinging_active variable assures that only one ping "task" is
        # active at a time.
        self._pinging_active = False
        self._pinglock = threading.Lock()
        self._schedule_ping(self.pingdelay)
        self.update_timestamp()

    def check_cookie(self, cookie):
        return (cookie == self.cookie)

    def busy(self):
        """Indicates whether the slave has a current assignment."""
        return (self._assignment is not None)

    def pop_assignment(self):
        """Removes and returns the current assignment."""
        with self._assignment_lock:
            assignment = self._assignment
            self._assignment = None
            return assignment

    def current_assignment(self):
        """Returns the current assignment.

        Note that this could change in another thread (so be careful).
        You usually want pop_assignment instead.
        """
        return self._assignment

    def set_assignment(self, old_assignment, new_assignment):
        """Sets the assignment to new_assignment if the old_assignment matches.
        """
        with self._assignment_lock:
            if self._assignment == old_assignment:
                self._assignment = new_assignment
                return True
            else:
                return False

    def prepare_assignment(self, assignment, datasets):
        """Sets up an RPC request to make the slave work on the assignment.

        Called from the Runner.  Note that the assignment will _not_ actually
        happen until `send_assignment` is subsequently called.  This is the
        responsibility of the caller.
        """
        success = self.set_assignment(None, assignment)
        assert success
        dataset_id, task_index = assignment

        logger.debug('Assigning task to slave %s: %s, %s'
                % (self.id, dataset_id, task_index))

        dataset = datasets[dataset_id]
        task = dataset.get_task(task_index, datasets, '')
        task_args = task.to_args()

        with self._rpc_lock:
            assert self._rpc_args is None
            self._rpc_func = self._rpc.start_task
            self._rpc_args = task_args + (self.cookie,)

    def send_assignment(self):
        with self._rpc_lock:
            if not self.alive():
                logger.warning('Canceling RPC call because slave %s is no'
                        ' longer alive.' % self.id)
                return

            logger.debug('Sending assignment to slave %s: %s, %s'
                    % (self.id, self._rpc_args[2], self._rpc_args[3]))
            try:
                success = self._rpc_func(*self._rpc_args)
            except Fault as f:
                logger.error('Fault in RPC call to slave %s: %s'
                        % (self.id, f.faultString))
                success = False
            except ProtocolError as e:
                logger.error('Protocol error in RPC call to slave %s: %s'
                        % (self.id, e.errmsg))
                success = False
            except socket.timeout:
                logger.error('Timeout in RPC call to slave %s' % self.id)
                success = False
            except socket.error as e:
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
            if self._state not in ('alive', 'exiting'):
                # Note: the master may disconnect the slave while remove
                # requests are still pending--this isn't really a bad thing.
                return

            logger.debug('Sending remove request to slave %s: %s, %s'
                    % (self.id, dataset_id, source))
            try:
                self._rpc.remove(dataset_id, source, delete, self.cookie)
                success = True
            except Fault as f:
                logger.error('Fault in remove call to slave %s: %s'
                        % (self.id, f.faultString))
                success = False
            except ProtocolError as e:
                logger.error('Protocol error in remove call to slave %s: %s'
                        % (self.id, e.errmsg))
                success = False
            except socket.timeout:
                logger.error('Timeout in remove call to slave %s' % self.id)
                success = False
            except socket.error as e:
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
            logger.warning('Resurrected slave %s (%s)' %
                    (self.id, self.host))
            with self._pinglock:
                self._state = 'alive'
                restart_pinging = not self._pinging_active
                if restart_pinging:
                    self._pinging_active = True
            if restart_pinging:
                self._schedule_ping(self.pingdelay)
            return True
        else:
            return False

    def ping(self):
        """Ping the slave and schedule a follow-up ping."""
        # The only place where we can change from not alive to alive is in
        # resurrect, which holds the pinglock to ensure that we don't
        # accidentally stop pinging during a resurrect.
        if not self.alive():
            with self._pinglock:
                if not self.alive():
                    self._pinging_active = False
                    return

        delta = time.time() - self.timestamp
        if delta < self.pingdelay:
            self._schedule_ping(self.pingdelay - delta)
            return

        if not self._rpc_lock.acquire(False):
            # RPC socket busy; try again later.
            self._schedule_ping(self.pingdelay)
            return

        try:
            logger.debug('Sending ping to slave %s.' % self.id)
            self._rpc.ping(self.cookie)
            success = True
        except Fault as f:
            logger.error('Fault in ping to slave %s: %s'
                    % (self.id, f.faultString))
            success = False
        except ProtocolError as e:
            logger.error('Protocol error in ping to slave %s: %s'
                    % (self.id, e.errmsg))
            success = False
        except socket.timeout:
            logger.error('Timeout in ping to slave %s' % self.id)
            success = False
        except socket.error as e:
            logger.error('Socket error in ping to slave %s: %s'
                    % (self.id, e.args[1]))
            success = False
        finally:
            self._rpc_lock.release()

        if success:
            self.update_timestamp()
            self._schedule_ping(self.pingdelay)
        else:
            # Mark pinging as inactive _before_ setting slave as failed.
            self._pinging_active = False
            self.critical_failure()

    def _schedule_ping(self, delay=None):
        """Schedules a ping to occur after the given delay.

        Ensures that the ping keeps repeating, i.e., when a ping finishes,
        a new ping is immediately scheduled.
        """
        logger.debug('Scheduling a ping to slave %s.' % self.id)
        self._pinging_active = True
        self.chore_queue.do(self.ping, delay=delay)

    def disconnect(self, write_pipe=None):
        """Disconnect the slave by sending a quit request."""
        if self._state not in ('exiting', 'exited'):
            self._state = 'exiting'
            self.chore_queue.do(self.send_exit, (write_pipe,))

    def send_exit(self, write_pipe=None):
        with self._rpc_lock:
            try:
                logger.debug('Sending a exit request to slave %s' % self.id)
                self._rpc.exit(self.cookie)
            except Fault as f:
                logger.error('Fault in exit to slave %s: %s'
                        % (self.id, f.faultString))
            except ProtocolError as e:
                logger.error('Protocol error in exit to slave %s: %s'
                        % (self.id, e.errmsg))
            except socket.timeout:
                logger.error('Timeout in exit to slave %s' % self.id)
            except socket.error as e:
                logger.error('Socket error in exit to slave %s: %s'
                        % (self.id, e.args[1]))
            self._state = 'exited'
            self._rpc = None

        if write_pipe is not None:
            os.write(write_pipe, b'\0')

    def __repr__(self):
        return ('RemoteSlave(%s, %s, %s, %s, %s)' % (self.id, self.host,
            self.port, self.cookie, repr(self.slaves)))


class Slaves(object):
    """List of remote slaves."""
    def __init__(self, sched_pipe, chore_queue, rpc_timeout, pingdelay):
        self._sched_pipe = sched_pipe
        self.chore_queue = chore_queue
        self.rpc_timeout = rpc_timeout
        self.pingdelay = pingdelay

        self._lock = threading.Lock()
        self._next_slave_id = 0
        self._slaves = {}
        self._accepting_new_slaves = True

        # Note that collections.deque is documented to be thread-safe.
        self._changed_slaves = collections.deque()
        self._results = collections.deque()
        self._failed_tasks = collections.deque()

    def trigger_sched(self):
        """Wakes up the runner for scheduling by sending it a byte."""
        os.write(self._sched_pipe, b'\0')

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
            if not self._accepting_new_slaves:
                return None
            slave_id = self._next_slave_id
            self._next_slave_id += 1
            slave = RemoteSlave(slave_id, host, slave_port, cookie, self)
            self._slaves[slave_id] = slave
        return slave

    def slave_ready(self, slave):
        if not slave.alive():
            if not slave.resurrect():
                return

        if slave.busy():
            logger.error('Slave %s reported ready but has an assignment; '
                    'check the slave logs for errors.' % slave.id)

        self._changed_slaves.append(slave)

        self.trigger_sched()

    def slave_result(self, slave, dataset_id, source, urls):
        success = slave.set_assignment((dataset_id, source), None)
        assert success
        self._results.append((slave, dataset_id, source, urls))
        self._changed_slaves.append(slave)
        self.trigger_sched()

    def slave_failed(self, slave, dataset_id, task_index):
        success = slave.set_assignment((dataset_id, task_index), None)
        assert success
        self._failed_tasks.append((dataset_id, task_index))
        self._changed_slaves.append(slave)
        self.trigger_sched()

    def slave_dead(self, slave):
        self._changed_slaves.append(slave)
        self.trigger_sched()

    def get_results(self):
        """Return and reset the list of results: (taskid, urls) pairs."""
        results = []
        while True:
            try:
                results.append(self._results.popleft())
            except IndexError:
                return results

    def get_changed_slaves(self):
        """Return and reset the list of changed slaves."""
        changed = set()
        while True:
            try:
                changed.add(self._changed_slaves.pop())
            except IndexError:
                return changed

    def get_failed_tasks(self):
        """Return and reset the list of changed slaves."""
        failed_tasks = []
        while True:
            try:
                failed_tasks.append(self._failed_tasks.popleft())
            except IndexError:
                return failed_tasks

    def disconnect_all(self):
        """Sends an exit request to the slaves and waits for completion."""
        with self._lock:
            self._accepting_new_slaves = False

        # Each slave writes to the write_pipe when the disconnect completes.
        read_pipe, write_pipe = os.pipe()
        for slave in self._slaves.values():
            slave.disconnect(write_pipe)

        keep_going = False
        for slave in self._slaves.values():
            if not slave.exited():
                keep_going = True
                break

        while keep_going:
            # The actual data read is irrelevant--this just lets us block.
            os.read(read_pipe, 4096)
            keep_going = False
            for slave in self._slaves.values():
                if not slave.exited():
                    keep_going = True
                    break

    def __len__(self):
        """Returns the total number of slaves (including dead slaves)."""
        return len(self._slaves)


class IdleSlaves(object):
    """A priority-queue-like container of Slave objects.

    Attributes:
        _host_map: A map from a host to the corresponding set of slaves.
        _counter: A dictionary that maps a count c to a set of hosts with c
            idle slaves.
    """
    def __init__(self):
        self._host_map = collections.defaultdict(set)
        self._counter = collections.defaultdict(set)
        self._all_slaves = set()
        self._max_count = 0

    def add(self, slave):
        host = slave.host

        # Remove the host's slave set from the counter.
        slave_set = self._host_map[host]
        self._remove_from_counter(host, len(slave_set))

        # Add the host to the slave_set.
        slave_set.add(slave)
        self._all_slaves.add(slave)

        # Reinsert the slave_set with its new count.
        self._add_to_counter(host, len(slave_set))

    def remove(self, slave):
        """Remove the given slave from the set.

        If it is not a member, raise a KeyError.
        """
        self._all_slaves.remove(slave)
        host = slave.host

        # Find the host's slave set and remove it from the counter.
        slave_set = self._host_map[host]
        self._remove_from_counter(host, len(slave_set))

        # Remove the host from the slave_set.
        slave_set.remove(slave)

        # Reinsert the slave_set with its new count.
        self._add_to_counter(host, len(slave_set))

    def discard(self, slave):
        """Remove the given slave from the set, if present."""
        self._all_slaves.discard(slave)
        host = slave.host

        # Find the host's slave set and remove it from the counter.
        slave_set = self._host_map[host]
        self._remove_from_counter(host, len(slave_set))

        # Discard the host from the slave_set.
        slave_set.discard(slave)

        # Reinsert the slave_set with its new count.
        self._add_to_counter(host, len(slave_set))

    def pop(self):
        """Remove and return a slave from the most idle host."""
        # Find and remove a host with the maximum number of idle slaves.
        counter_set = self._counter[self._max_count]
        host = counter_set.pop()
        if not counter_set:
            self._max_count -= 1

        # Pop a slave and reinsert the host (with its new count).
        slave_set = self._host_map[host]
        slave = slave_set.pop()
        self._all_slaves.remove(slave)
        self._add_to_counter(host, len(slave_set))
        return slave

    def _add_to_counter(self, host, host_size):
        counter_set = self._counter[host_size]
        counter_set.add(host)
        if host_size > self._max_count:
            self._max_count = host_size

    def _remove_from_counter(self, host, host_size):
        if host_size != 0:
            counter_set = self._counter[host_size]
            counter_set.remove(host)
            if not counter_set:
                self._max_count -= 1

    def __nonzero__(self):
        return self._max_count > 0

    def __bool__(self):
        return self._max_count > 0

    def __contains__(self, slave):
        return slave in self._all_slaves

# vim: et sw=4 sts=4

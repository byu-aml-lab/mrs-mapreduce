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

"""Mrs Slave

The Mrs Slave runs in two processes: the main process and the worker process.
The main process has a main thread, a slave thread and an rpc thread.

The main thread doesn't really do anything.  It starts the other two threads
and waits for them to finish.  If the user hits CTRL-C, the main thread will
be interrupted, and it will shut down the event thread.  The only reason that
the main thread exists at all is to deal with signals.

The worker process executes the user's map function and reduce function.
That's it.  It just does what the main process tells it to.  The worker
process is terminated when the main process quits.
"""

# Number of ping timeouts before giving up:
PING_ATTEMPTS = 50

COOKIE_LEN = 8

import datetime
import multiprocessing
import optparse
import select
import socket
import tempfile
import threading
import worker

from . import registry
from . import rpc
from . import util
from .version import VERSION

from logging import getLogger
logger = getLogger('mrs')


class Slave(object):
    """State of a Mrs slave"""

    def __init__(self, program_class, master_url, local_shared, pingdelay,
            timeout):
        self.program_class = program_class
        self.master_url = master_url
        self.local_shared = local_shared
        self.pingdelay = pingdelay
        self.timeout = timeout

        self.request_pipe_slave, self.request_pipe_worker = (
                multiprocessing.Pipe())
        self.quit_pipe_recv, self.quit_pipe_send = multiprocessing.Pipe(False)

        self.id = None
        self.cookie = util.random_string(COOKIE_LEN)
        self.timestamp = None
        self.watchdog_stamp = None
        self.port = None
        self.master_rpc = None

        self.setup_complete = False
        self.current_request = None

    def run(self):
        self.start_rpc_server_thread()
        self.master_rpc = rpc.ServerProxy(self.master_url, self.timeout)

        result = self.signin()
        if not result:
            return
        self.id, jobdir, opts, args = result
        default_dir = self.init_default_dir(jobdir)

        # Tell the Worker to run the user_setup function and wait for
        # a response.
        if not self.worker_setup(opts, args, default_dir):
            return

        self.setup_complete = True

        # TODO: start a ping and/or watchdog thread

        self.report_ready()
        self.workloop()

    def start_rpc_server_thread(self):
        rpc_interface = SlaveInterface(self)
        rpc_server = rpc.Server(('', 0), rpc_interface)
        _, self.port = rpc_server.socket.getsockname()

        rpc_thread = threading.Thread(target=rpc_server.serve_forever,
                name='RPC Server')
        rpc_thread.daemon = True
        rpc_thread.start()

    def signin(self):
        """Start Slave RPC Server and sign in to master.

        Returns (slave_id, jobdir, opts, args): the slave id, job
        directory, options object, and args list given by the server.
        """
        cookie = self.cookie
        program_hash = registry.object_hash(self.program_class)

        try:
            slave_id, jobdir, optdict, args = self.master_rpc.signin(VERSION,
                    cookie, self.port, program_hash)
        except socket.error, e:
            msg = e.args[1]
            logger.critical('Unable to contact master: %s' % msg)
            return None

        if slave_id < 0:
            logger.critical('Master rejected signin.')
            return None

        # Parse the opts given by the master.
        opts = optparse.Values(optdict)

        return slave_id, jobdir, opts, args

    def init_default_dir(self, jobdir):
        if self.local_shared:
            util.try_makedirs(self.local_shared)
            return tempfile.mkdtemp(dir=self.local_shared, prefix='mrs_slave_')
        else:
            hostname, _, _ = socket.gethostname().partition('.')
            return tempfile.mkdtemp(dir=jobdir, prefix=hostname)

    def report_ready(self):
        """Report to the master that we are ready to accept tasks.

        This is the callback after user_setup is called.
        """
        # TODO: make this try a few times if there's a timeout
        try:
            self.master_rpc.ready(self.id, self.cookie)
        except socket.error, e:
            msg = e.args[0]
            logger.critical('Failed to report due to network error: %s' % msg)

        logger.info('Connected to master.')
        self.update_timestamp()

    def worker_setup(self, opts, args, default_dir):
        request = worker.WorkerSetupRequest(opts, args, default_dir)
        self.request_pipe_slave.send(request)
        response = self.request_pipe_slave.recv()
        if isinstance(response, worker.WorkerSetupSuccess):
            return True
        if isinstance(response, worker.WorkerFailure):
            msg = 'Exception in Worker Setup: %s' % response.exception
            logger.critical(msg)
            msg = 'Traceback: %s' % response.traceback
            logger.error(msg)
            return False
        else:
            raise RuntimeError('Invalid message type.')

    def workloop(self):
        """Repeatedly process completed requests.

        The RPC thread submits requests to the worker (via submit_request),
        but this workloop processes their completion.
        """
        poll = select.poll()
        poll.register(self.request_pipe_slave, select.POLLIN)
        poll.register(self.quit_pipe_recv, select.POLLIN)

        while True:
            for fd, event in poll.poll():
                if fd == self.quit_pipe_recv.fileno():
                    return
                else:
                    self.process_one_response()

    def process_one_response(self):
        """Reads a single response from the request pipe."""

        response = self.request_pipe_slave.recv()
        assert self.current_request is not None
        self.current_request = None
        if isinstance(response, worker.WorkerSuccess):
            self.master_rpc.done(self.id, response.dataset_id,
                    response.source, response.outurls, self.cookie)
        elif isinstance(response, worker.WorkerFailure):
            msg = 'Exception in Worker: %s' % response.exception
            logger.critical(msg)
            msg = 'Traceback: %s' % response.traceback
            logger.error(msg)
        else:
            assert False

    def submit_request(self, request):
        """Submit the given request to the worker.

        Returns a boolean indicating whether the request was accepted.  Called
        from the RPC thread.
        """
        if self.current_request is None:
            self.current_request = request
            self.request_pipe_slave.send(request)
            return True
        else:
            return False

    def update_timestamp(self):
        """Set the timestamp to the current time."""
        self.timestamp = datetime.datetime.utcnow()

    def get_timestamp(self):
        """Report the most recent timestamp."""
        return self.timestamp

    def check_cookie(self, cookie):
        if cookie != self.cookie:
            raise CookieValidationError

    def register_worker(self, worker):
        """Called by the worker so the Slave can have a reference to it."""
        self.worker = worker

    def quit(self):
        """Called to tell the slave to quit."""
        self.quit_pipe_send.send(1)


class SlaveInterface(object):
    """Public XML RPC Interface

    Note that any method beginning with "xmlrpc_" will be exposed to
    remote hosts.
    """
    def __init__(self, slave):
        self.slave = slave

    def xmlrpc_start_map(self, dataset_id, source, inputs, func_name,
            part_name, splits, outdir, extension, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a Map assignment from the master.')
        request = worker.WorkerMapRequest(dataset_id, source, inputs,
                func_name, part_name, splits, outdir, extension)
        return self.slave.submit_request(request)

    def xmlrpc_start_reduce(self, dataset_id, source, inputs, func_name,
            part_name, splits, outdir, extension, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a Reduce assignment from the master.')
        request = worker.WorkerReduceRequest(dataset_id, source, inputs,
                func_name, part_name, splits, outdir, extension)
        return self.slave.submit_request(request)

    def xmlrpc_quit(self, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a request to quit from the master.')
        # We delay before actually stopping because we need to make sure that
        # the response gets sent back.
        self.slave.quit()
        return True

    def xmlrpc_ping(self, cookie):
        """Master checking if we're still here.
        """
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.debug('Received a ping from the master.')
        return True


class CookieValidationError(Exception):
    pass


# vim: et sw=4 sts=4

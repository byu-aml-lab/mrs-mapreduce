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
process is terminated when the main process exits.
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
import urlparse
import worker

from . import bucket
from . import http
from . import registry
from . import util
from .version import VERSION

from logging import getLogger
logger = getLogger('mrs')


class Slave(object):
    """State of a Mrs slave

    Attributes:
        _outdirs: map from a (dataset_id, source) pair to an output directory
    """

    def __init__(self, program_class, master_url, local_shared, pingdelay,
            timeout):
        self.program_class = program_class
        self.master_url = master_url
        self.local_shared = local_shared
        self.pingdelay = pingdelay
        self.timeout = timeout

        self.request_pipe_slave, self.request_pipe_worker = (
                multiprocessing.Pipe())
        self.exit_pipe_recv, self.exit_pipe_send = multiprocessing.Pipe(False)

        self.id = None
        self.cookie = util.random_string(COOKIE_LEN)
        self.timestamp = None
        self.watchdog_stamp = None
        self.rpc_port = None
        self.bucket_port = None
        self.master_rpc = None
        self.url_converter = None

        self.setup_complete = False
        self.current_request_id = None
        self._outdirs = {}
        self._outdirs_lock = threading.Lock()

    def run(self):
        self.start_rpc_server_thread()
        self.master_rpc = http.ServerProxy(self.master_url, self.timeout)

        result = self.signin()
        if not result:
            return
        self.id, addr, jobdir, opts, args = result
        default_dir = self.init_default_dir(jobdir)

        if self.local_shared:
            self.start_bucket_server_thread(default_dir)
            self.url_converter = bucket.URLConverter(addr, self.bucket_port,
                    default_dir)

        # Tell the Worker to run the user_setup function and wait for
        # a response.
        if not self.worker_setup(opts, args, default_dir):
            return

        self.setup_complete = True

        # TODO: start a ping and/or watchdog thread

        self.report_ready()
        try:
            self.workloop()
        finally:
            util.remove_recursive(default_dir)

    def start_rpc_server_thread(self):
        rpc_interface = SlaveInterface(self)
        rpc_server = http.RPCServer(('', 0), rpc_interface)
        _, self.rpc_port = rpc_server.socket.getsockname()

        rpc_thread = threading.Thread(target=rpc_server.serve_forever,
                name='RPC Server')
        rpc_thread.daemon = True
        rpc_thread.start()

    def start_bucket_server_thread(self, default_dir):
        bucket_server = http.ThreadingBucketServer(('', 0), default_dir)
        _, self.bucket_port = bucket_server.socket.getsockname()

        bucket_thread = threading.Thread(target=bucket_server.serve_forever,
                name='Bucket Server')
        bucket_thread.daemon = True
        bucket_thread.start()

    def signin(self):
        """Start Slave RPC Server and sign in to master.

        Returns (slave_id, jobdir, opts, args): the slave id, job
        directory, options object, and args list given by the server.
        """
        cookie = self.cookie
        program_hash = registry.object_hash(self.program_class)

        try:
            slave_id, addr, jobdir, optdict, args = self.master_rpc.signin(
                    VERSION, cookie, self.rpc_port, program_hash)
        except socket.error, e:
            msg = e.args[1]
            logger.critical('Unable to contact master: %s' % msg)
            return None

        if slave_id < 0:
            logger.critical('Master rejected signin.')
            return None

        # Parse the opts given by the master.
        opts = optparse.Values(optdict)

        return slave_id, addr, jobdir, opts, args

    def init_default_dir(self, jobdir):
        if self.local_shared:
            util.try_makedirs(self.local_shared)
            directory = self.local_shared
            prefix = 'mrs_slave_'
        else:
            hostname, _, _ = socket.gethostname().partition('.')
            directory = jobdir
            prefix = hostname
        return tempfile.mkdtemp(dir=directory, prefix=prefix)

    def report_ready(self):
        """Report to the master that we are ready to accept tasks.

        This is the callback after user_setup is called.
        """
        assert self.current_request_id is None

        # TODO: make this try a few times if there's a timeout
        try:
            self.master_rpc.ready(self.id, self.cookie)
        except socket.error, e:
            msg = e.args[0]
            logger.critical('Failed to report due to network error: %s' % msg)

        logger.info('Reported ready to master.')
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
        poll.register(self.exit_pipe_recv, select.POLLIN)

        while True:
            for fd, event in poll.poll():
                if fd == self.exit_pipe_recv.fileno():
                    request = worker.WorkerQuitRequest()
                    self.request_pipe_slave.send(request)
                    return
                else:
                    self.process_one_response()

    def process_one_response(self):
        """Reads a single response from the request pipe."""

        r = self.request_pipe_slave.recv()
        if isinstance(r, worker.WorkerSuccess):
            assert self.current_request_id == r.request_id
            self.current_request_id = None
            self.add_output_dir(r.dataset_id, r.source, r.outdir)
            outurls = r.outurls
            if self.url_converter:
                convert_url = self.url_converter.local_to_global
                outurls = [(s, convert_url(url)) for s, url in outurls]
            self.master_rpc.done(self.id, r.dataset_id, r.source, outurls,
                    self.cookie)
        elif isinstance(r, worker.WorkerFailure):
            if self.current_request_id == r.request_id:
                self.current_request_id = None
                self.report_ready()
            msg = 'Exception in Worker: %s' % r.exception
            logger.critical(msg)
            msg = 'Traceback: %s' % r.traceback
            logger.error(msg)
        else:
            assert False

    def submit_request(self, request):
        """Submit the given request to the worker.

        If one_at_a_time is specified, then no other one_at_time requests can
        be accepted until the current task finishes.  Returns a boolean
        indicating whether the request was accepted.

        Called from the RPC thread.
        """
        if (isinstance(request, worker.WorkerMapRequest) or
                isinstance(request, worker.WorkerReduceRequest)):
            if self.current_request_id is not None:
                return False
            self.current_request_id = request.id()

        self.request_pipe_slave.send(request)
        return True

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

    def add_output_dir(self, dataset_id, source, outdir):
        """Stores the output directory (for subsequent deletion)."""
        with self._outdirs_lock:
            self._outdirs[dataset_id, source] = outdir

    def pop_output_dir(self, dataset_id, source):
        """Return and remove the output directory for the specified source."""
        with self._outdirs_lock:
            outdir = self._outdirs[dataset_id, source]
            del self._outdirs[dataset_id, source]
        return outdir

    def exit(self):
        """Called to tell the slave to exit."""
        self.exit_pipe_send.send(1)


class SlaveInterface(object):
    """Public XML RPC Interface

    Note that any method beginning with "xmlrpc_" will be exposed to
    remote hosts.
    """
    def __init__(self, slave):
        self.slave = slave

    @http.uses_host
    def xmlrpc_start_map(self, dataset_id, source, inputs, func_name,
            part_name, splits, outdir, extension, cookie, host=None):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a Map assignment from the master.')

        if self.slave.url_converter:
            convert_url = self.slave.url_converter.global_to_local
            inputs = [convert_url(url, host) for url in inputs]

        request = worker.WorkerMapRequest(dataset_id, source, inputs,
                func_name, part_name, splits, outdir, extension)
        return self.slave.submit_request(request)

    @http.uses_host
    def xmlrpc_start_reduce(self, dataset_id, source, inputs, func_name,
            part_name, splits, outdir, extension, cookie, host=None):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a Reduce assignment from the master.')

        if self.slave.url_converter:
            convert_url = self.slave.url_converter.global_to_local
            inputs = [convert_url(url, host) for url in inputs]

        request = worker.WorkerReduceRequest(dataset_id, source, inputs,
                func_name, part_name, splits, outdir, extension)
        return self.slave.submit_request(request)

    def xmlrpc_remove(self, dataset_id, source, delete, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received remove request from master: %s, %s'
                % (dataset_id, source))
        if delete:
            outdir = self.slave.pop_output_dir(dataset_id, source)
            request = worker.WorkerRemoveRequest(outdir)
            success = self.slave.submit_request(request)
        else:
            success = True

        return success

    def xmlrpc_exit(self, cookie):
        self.slave.check_cookie(cookie)
        self.slave.update_timestamp()
        logger.info('Received a request to exit from the master.')
        # We delay before actually stopping because we need to make sure that
        # the response gets sent back.
        self.slave.exit()
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

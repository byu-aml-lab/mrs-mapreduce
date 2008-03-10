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

"""Mrs. Twist (no relation to Oliver)

Helper functions and classes for Python Twisted.
"""

PING_INTERVAL = 5.0
PING_STDDEV = 0.1

import threading
from twisted.internet import reactor
import xmlrpclib
from twisted.internet import defer
from twisted.web import server, xmlrpc

# TODO: Once the master uses TwistedThread for everything, remove setDaemon.

class TwistedThread(threading.Thread):
    """The Twisted thread handles all network communication and slow IO.
    
    The main responsibilities of the twisted thread are:
    - download files
    - periodically ping clients or server
    - XMLRPC server
    - XMLRPC client
    - serve files to peers over http
    """

    def __init__(self, **kwds):
        threading.Thread.__init__(self, **kwds)

        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

    def run(self):
        """Run is called when the thread is started."""
        # If the Twisted reactor is not the primary thread, then we need to
        # disable installSignalHandlers.  If installSignalHandlers is set,
        # then Twisted will override SIGINT, SIGBREAK, and SIGCHLD.  The
        # primary thread will want to catch SIGINT and SIGBREAK.  Also note
        # that Twisted's SIGCHLD handler apparently conflicts with Python's
        # subprocess module.
        reactor.run(installSignalHandlers=0)

    def shutdown(self):
        """Ask the reactor thread to stop.
        
        After shutdown, the thread still needs to be joined.
        """
        reactor.callFromThread(reactor.stop)

class FromThreadProxy(object):
    """XMLRPC Proxy that operates in a separate thread from the reactor."""

    def __init__(self, url):
        from twisted.web import xmlrpc
        from util import rpc_url
        self.proxy = xmlrpc.Proxy(rpc_url(url))

    def blocking_call(self, *args):
        """Make a blocking XML RPC call to a remote server."""
        # pause between 'blocking call' and 'calling'
        deferred = self.deferred_call(*args)
        result = block(deferred)
        return result

    def deferred_call(self, *args):
        """Make a deferred XML RPC call to a remote server."""
        deferred = reactor_call(self.proxy.callRemote, *args)
        return deferred

    def callRemote(self, *args):
        """Make a deferred XML RPC call *from the reactor thread*."""
        return self.proxy.callRemote(*args)


# TODO: make it so the slave can use this, too
class PingTask(object):
    """Periodically make an XML RPC call to the ping procedure."""
    def __init__(self, slave):
        self.slave = slave
        self.running = False
        self._callid = None

        # Last time that we checked to see if the slave is alive:
        self.timestamp = self.slave.timestamp

    def start(self):
        assert(not self.running)
        self.running = True
        reactor.callFromThread(self._schedule_next)

    def stop(self):
        assert(self.running)
        self.running = False
        reactor.callFromThread(self._cancel)

    def _schedule_next(self):
        """Set up the next call.  Randomly adjust the delay.
        
        This _must_ be called from the reactor thread.
        """
        # we can't schedule a new one if the old one hasn't executed yet.
        assert(self._callid is None)
        import random
        delay = random.normalvariate(PING_INTERVAL, PING_STDDEV)
        self._callid = reactor.callLater(delay, self._task)

    def _cancel(self):
        if self._callid:
            self._callid.cancel()
            self._callid = None

    def _update_timestamp(self, activity=False):
        """Update our timestamp of the last time we checked on the slave.

        If we have received communication from the slave (activity is True),
        update self.slave's timestamp, too.
        """
        if activity:
            self.slave.update_timestamp()
            self.timestamp = self.slave.timestamp
        else:
            from datetime import datetime
            self.timestamp = datetime.utcnow()

    def _task(self):
        """The PingTask's repeatedly called function.
        
        Ping the slave if it's necessary to do so.
        """
        self._callid = None
        if self.slave.timestamp_since(self.timestamp):
            self._update_timestamp()
            self._schedule_next()
        else:
            deferred = self.slave.rpc.callRemote('ping')
            deferred.addCallback(self._callback)
            deferred.addErrback(self._errback)

    def _callback(self, value):
        """Called when the slave responds to a ping."""
        self._update_timestamp(True)
        self._schedule_next()

    def _errback(self, failure):
        """Called when the slave fails to respond to a ping."""
        self._update_timestamp()
        self.slave.rpc_failure()
        self.running = False
        self._cancel()


def reactor_call(f, *args):
    """Call the given function inside the reactor.

    Return the result.
    """
    target = []
    event = threading.Event()
    reactor.callFromThread(_reactor_call2, event, target, f, *args)
    # FIXME: this operation occasionally hangs for a second or two when
    # there's a lot of IO.  The reason seems to be that there are two many
    # IO-related callbacks, so the reactor can get to our request quickly.
    # The solution is probably to redo the IO using Twisted's producer
    # and consumer interfaces.
    event.wait()
    deferred = target[0]
    return deferred

def _reactor_call2(event, target, f, *args):
    """Call the given function.

    Append the result to the target list.  WARNING: this function should only
    be called within the reactor thread.
    """
    result = f(*args)
    target.append(result)
    event.set()


class ErrbackException(RuntimeError):
    def __init__(self, failure):
        self.failure = failure

    def __str__(self):
        return str(self.failure)

def notify_callback(value, event, target=None):
    """Notifies a event variable when callback occurs.
    
    If a target list is given, the result value will be appended to it.
    """
    target.append(value)
    event.set()

def notify_errback(failure, event, target=None):
    """Sets an event when errback occurs.
    
    If a target list is given, the failure will be appended to it.
    """
    target.append(failure)
    event.set()

def block(deferred):
    """Block on a deferred and return its result.

    Note that the reactor must be in another thread.
    """
    vals = []
    errs = []
    event = threading.Event()
    reactor.callFromThread(deferred.addCallback, notify_callback, event, vals)
    reactor.callFromThread(deferred.addErrback, notify_callback, event, errs)
    event.wait()
    if vals:
        return vals[0]
    elif errs:
        raise ErrbackException(errs[0])
    else:
        assert(False)

class KeywordsXMLRPC(xmlrpc.XMLRPC):
    """Extension of XMLRPC which passes the client to RPC methods."""

    def keywords(self, request):
        return {}

    # We redefine the render function to send in the named parameters.
    def render(self, request):
        request.content.seek(0, 0)
        args, functionPath = xmlrpclib.loads(request.content.read())
        kwds = self.keywords(request)
        try:
            function = self._getFunction(functionPath)
        except Fault, f:
            self._cbRender(f, request)
        else:
            request.setHeader("content-type", "text/xml")
            defer.maybeDeferred(function, *args, **kwds).addErrback(
                self._ebRender
            ).addCallback(
                self._cbRender, request
            )
        return server.NOT_DONE_YET


# vim: et sw=4 sts=4

# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
# more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Mrs.  If not, see <http://www.gnu.org/licenses/>.

import threading
from twisted.internet import reactor

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
        self.setDaemon(True)

    def run(self):
        """Run is called when the thread is started."""
        reactor.run(installSignalHandlers=0)

    def shutdown(self):
        """Shutdown the reactor and wait for the thread to terminate."""
        reactor.callFromThread(reactor.stop)
        self.join()

class FromThreadProxy(object):
    """XMLRPC Proxy that operates in a separate thread from the reactor."""

    def __init__(self, url):
        from twisted.web import xmlrpc
        from util import rpc_url
        self.proxy = xmlrpc.Proxy(rpc_url(url))

    def blocking_call(self, *args):
        """Make a blocking XMLRPC call to a remote server."""
        # pause between 'blocking call' and 'calling'
        deferred = self.deferred_call(*args)
        result = block(deferred)
        return result

    def deferred_call(self, *args):
        """Make a deferred XMLRPC call to a remote server."""
        deferred = self._reactor_call(self.proxy.callRemote, *args)
        return deferred

    def _reactor_call(self, f, *args):
        """Call the given function inside the reactor.

        Save the result to self._reactor_result.
        """
        target = []
        condition = threading.Condition()
        condition.acquire()
        reactor.callFromThread(self._reactor_call2, condition, target, f, *args)
        # FIXME: this operation occasionally hangs for a second or two when
        # there's a lot of IO.  The reason seems to be that there are two many
        # IO-related callbacks, so the reactor can get to our request quickly.
        # The solution is probably to redo the IO using Twisted's producer
        # and consumer interfaces.
        condition.wait()
        condition.release()
        deferred = target[0]
        return deferred

    def _reactor_call2(self, condition, target, f, *args):
        """Call the given function.

        Save the result to self._reactor_result.  WARNING: this function
        should only be called within the reactor thread.
        """
        result = f(*args)
        condition.acquire()
        target.append(result)
        condition.notify()
        condition.release()


class ErrbackException(RuntimeError):
    def __init__(self, failure):
        self.failure = failure

    def __str__(self):
        return str(self.failure)

def notify_callback(value, condition, target=None):
    """Notifies a condition variable when callback occurs.
    
    If a target list is given, the result value will be appended to it.
    """
    condition.acquire()
    target.append(value)
    condition.notify()
    condition.release()

def notify_errback(failure, condition, target=None):
    """Notifies a condition variable when errback occurs.
    
    If a target list is given, the failure will be appended to it.
    """
    condition.acquire()
    target.append(failure)
    condition.notify()
    condition.release()

def block(deferred):
    """Block on a deferred and return its result.

    Note that the reactor must be in another thread.
    """
    vals = []
    errs = []
    cond = threading.Condition()
    cond.acquire()
    reactor.callFromThread(deferred.addCallback, notify_callback, cond, vals)
    reactor.callFromThread(deferred.addErrback, notify_callback, cond, errs)
    cond.wait()
    cond.release()
    if vals:
        return vals[0]
    elif errs:
        raise ErrbackException(errs[0])
    else:
        assert(False)

# vim: et sw=4 sts=4

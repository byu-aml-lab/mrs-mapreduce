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
from twisted.internet import defer

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


# TODO: make it so the slave can use this, too
class PingTask(object):
    """Periodically make an XML RPC call to the ping procedure.
    
    The `rpc` proxy is used to make ping requests.  The `success` function is
    called when a ping successfully completes, and the `failure` function is
    called when a ping fails.  The optional `last_activity` function is called
    to get the timestamp for the last time activity occurred.  This lets the
    PingTask be lazy and avoid a ping if some other form of communication
    already occurred.

    Note that the PingTask will continue running regardless of whether a ping
    succeeds or fails.  In other words, `success` and `failure` are
    responsible for stopping the PingTask if necessary.
    """
    def __init__(self, ping_args, rpc, success, failure, last_activity=None):
        self.ping_args = ping_args
        self.rpc = rpc
        self.success = success
        self.failure = failure
        self.last_activity = last_activity

        self.running = False
        self._callid = None

    def start(self):
        """Start or restart the ping task."""
        assert(not self.running)
        self.running = True
        self._update_timestamp()
        self._schedule_next()

    def stop(self):
        # FIXME: We have this `if self.running` here because apparently the
        # task can get stopped more than once.  This seems to be in the case
        # where there's another failure (like in rpc_failure in master.py).
        # We should really look into this and make sure that there's not some
        # other problem.
        if self.running:
            self.running = False
            self._cancel()
        else:
            print 'Warning: Tried to stop a PingTask twice!'

    def _update_timestamp(self):
        """Set the timestamp to the current time."""
        from datetime import datetime
        self.timestamp = datetime.utcnow()

    def _schedule_next(self):
        """Set up the next call.  Randomly adjust the delay.
        
        This _must_ be called from the reactor thread.
        """
        from datetime import datetime, timedelta
        from util import delta_seconds

        # we can't schedule a new one if the old one hasn't executed yet.
        assert(self._callid is None)

        now = datetime.utcnow()
        ping_interval = timedelta(seconds=PING_INTERVAL)
        since_last = now - self.timestamp

        base_delay = delta_seconds(ping_interval - since_last)
        if base_delay < 0:
            delay = 0
        else:
            import random
            delay = random.normalvariate(base_delay, PING_STDDEV)
            if delay < 0:
                delay = 0
        self._callid = reactor.callLater(delay, self._task)

    def _cancel(self):
        if self._callid:
            self._callid.cancel()
            self._callid = None

    def _task(self):
        """The PingTask's repeatedly called function.
        
        Ping the slave if it's necessary to do so.
        """
        self._callid = None

        # Check whether there has been more recent communication.
        recent_activity = False
        if self.last_activity:
            last = self.last_activity()
            if last > self.timestamp:
                self.timestamp = last
                recent_activity = True

        if recent_activity:
            self._schedule_next()
        else:
            deferred = self.rpc.callRemote(*self.ping_args)
            deferred.addCallback(self._callback)
            deferred.addErrback(self._errback)

    def _callback(self, value):
        """Called when the slave responds to a ping."""
        self._update_timestamp()
        self._schedule_next()
        # Call the user's callback:
        self.success()

    def _errback(self, error):
        """Called when the slave fails to respond to a ping."""
        self._schedule_next()
        # Call the user's callback:
        self.failure(error)


class GrimReaper(object):
    """Coordinate the death of threads.
    
    The reaper's `event` gets triggered when all of the threads need to quit.
    """
    def __init__(self):
        self.event = threading.Event()
        self.traceback = None

    def reap(self, exception=None):
        if exception is not None:
            import traceback
            self.traceback = traceback.format_exc()
        self.event.set()

    def wait(self):
        while not self.event.isSet():
            self.event.wait(100000)
        # Theoretically we should be able to do the following instead of the
        # above loop.  However, there's a Python bug where KeyboardInterrupt
        # can't interrupt waiting on a Lock.
        #self.event.wait()

def reactor_call(f, *args):
    """Call the given function inside the reactor.

    Return the result.
    """
    target = []
    event = threading.Event()
    reactor.callFromThread(_reactor_call2, event, target, f, *args)
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

# vim: et sw=4 sts=4

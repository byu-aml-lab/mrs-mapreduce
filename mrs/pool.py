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

"""Mrs. Thread Pool and Run Queue"""

from __future__ import with_statement

MIN_THREADS = 1
MAX_THREADS = 20

import heapq
import os
import Queue
import random
import sys
import threading
import time
import traceback

import logging
logger = logging.getLogger('mrs')
del logging


class ThreadPool(object):
    """Manages a pool of threads that retrieve and run items from the queue.

    Items in the queue are interpreted as (f, args) pairs.
    """
    def __init__(self, q):
        self.q = q
        self.threads = []
        self._cv = threading.Condition()
        # Counter represents the number of idle threads.
        self.counter = Counter(self._cv)

    def run(self):
        """Starts up and monitors threads."""
        with self._cv:
            while True:
                if ((self.counter.value() <= 0)
                        and (len(self.threads) < MAX_THREADS)):
                    self._new_thread()
                self._cv.wait()

    def _new_thread(self):
        function_caller = FunctionCaller(self.q, self.counter)
        t = threading.Thread(target=function_caller.run, name='Runner')
        t.daemon = True
        self.threads.append(t)
        self.counter.inc()
        t.start()


class Counter(object):
    def __init__(self, condition, value=0):
        self._cv = condition
        self._value = value

    def inc(self):
        with self._cv:
            self._value += 1
            if self._value <= 0:
                self._nonpositive_cv.notify_all()

    def dec(self):
        with self._cv:
            self._value -= 1
            if self._value <= 0:
                self._cv.notify_all()

    def value(self):
        return self._value


class FunctionCaller(object):
    def __init__(self, q, counter):
        self.q = q
        self.counter = counter

    def run(self):
        while True:
            result = self.q.get()
            if result:
                self.counter.dec()
                f, args = result
                try:
                    f(*args)
                except Exception as e:
                    tb = traceback.format_exc()
                    msg = 'Exception in thread pool: %s' % e
                    logger.critical(msg)
                    logger.error('Traceback: %s' % tb)
                self.counter.inc()


class RunQueue(object):
    """An unbounded time-based priority queue

    A priority queue structure that retrieves items at specific times.

    For the sake of simplicity, the RunQueue requires that reschedule() be
    called periodically.  The time_to_reschedule() method gives the number
    of seconds until the next call, and the new_earliest_fd file descriptor
    is written to whenever this time is reduced.
    """
    def __init__(self, new_earliest_fd):
        self._q = Queue.Queue()
        self._heap = []
        self._lock = threading.Lock()

        self._new_earliest_fd = new_earliest_fd
        self._earliest = None

    def do(self, f, args=(), delay=0):
        """Run a function with given args.

        The action will be performed after a delay (in seconds) if the option
        is specified.
        """
        item = f, args
        if delay:
            when = time.time() + delay
            with self._lock:
                heapq.heappush(self._heap, (when, item))

                if (self._earliest is None) or (when < self._earliest):
                    self._earliest = when
                    os.write(self._new_earliest_fd, '\0')
        else:
            self._put(item)

    def get(self, *args, **kwds):
        """Retrieve the next (f, args) pair from the queue.

        The options are the same as those provided by Queue.Queue.get.
        """
        return self._q.get(*args, **kwds)

    def time_to_reschedule(self):
        """Returns the number of seconds until reschedule should be called."""
        now = time.time()
        if self._earliest is None:
            return None
        else:
            return max(0, self._earliest - now)

    def reschedule(self):
        """Moves any pending items to the queue."""
        with self._lock:
            now = time.time()
            while True:
                if self._heap:
                    when, item = self._heap[0]
                else:
                    when = None

                if (when is not None) and (when <= now):
                    heapq.heappop(self._heap)
                    self._put(item)
                else:
                    break

            if self._heap:
                self._earliest, _ = self._heap[0]
            else:
                self._earliest = None

    def _put(self, item):
        """Put the given item on self._q.

        Unlike a simple self._q.put(item), this does not block
        KeyboardInterrupt.
        """
        while True:
            try:
                self._q.put(item, timeout=1)
                break
            except Queue.Full:
                pass


# vim: et sw=4 sts=4

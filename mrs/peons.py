# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Mrs. Chore Queue and Peon Thread"""

from __future__ import division

import collections
import heapq
import os
import random
import sys
import threading
import time
import traceback


import logging
logger = logging.getLogger('mrs')
del logging


class PeonThread(object):
    """The body of each peon thread.

    Pulls a (function, args) pair from the queue, applies the function to the
    args, and repeats.
    """
    def __init__(self, chore_queue):
        self.chore_queue = chore_queue

    def run(self):
        while True:
            result = self.chore_queue.get()
            if result:
                f, args = result
                try:
                    f(*args)
                except Exception as e:
                    tb = traceback.format_exc()
                    msg = 'Exception in thread pool: %s' % e
                    logger.critical(msg)
                    logger.error('Traceback: %s' % tb)


def start_peon_thread(chore_queue):
    logger.debug('Creating a new peon thread.')
    function_caller = PeonThread(chore_queue)
    t = threading.Thread(target=function_caller.run, name='Peon')
    t.daemon = True
    t.start()


class ChoreQueue(object):
    """An unbounded time-based priority queue of chores for peons.

    For the sake of simplicity, the ChoreQueue requires that reschedule() be
    called periodically.  The time_to_reschedule() method gives the number
    of seconds until the next call, and the new_earliest_fd file descriptor
    is written to whenever this time is reduced.
    """
    def __init__(self, new_earliest_fd):
        # Python's collections.deque is officially thread-safe.
        self._q = collections.deque()
        self._q_not_empty = threading.Condition(threading.Lock())
        self._heap = []
        self._heaplock = threading.Lock()

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
            with self._heaplock:
                heapq.heappush(self._heap, (when, item))

                if (self._earliest is None) or (when < self._earliest):
                    self._earliest = when
                    os.write(self._new_earliest_fd, b'\0')
        else:
            self._put(item)

    def do_many(self, items):
        """Run the given items, each of which is an (f, args) pair."""
        self._put_many(items)

    def get(self, *args, **kwds):
        """Retrieve the next (f, args) pair from the queue.

        The options are the same as those provided by queue.Queue.get.
        """
        # Avoid acquiring the lock if possible (to reduce contention).
        while True:
            try:
                return self._q.popleft()
            except IndexError:
                pass

            with self._q_not_empty:
                try:
                    return self._q.popleft()
                except IndexError:
                    self._q_not_empty.wait()

    def time_to_reschedule(self):
        """Returns the number of seconds until reschedule should be called."""
        earliest = self._earliest
        if earliest is None:
            return None
        else:
            now = time.time()
            return max(0, earliest - now)

    def reschedule(self):
        """Moves any pending items to the queue."""
        items = []

        with self._heaplock:
            now = time.time()
            while True:
                if self._heap:
                    when, item = self._heap[0]
                else:
                    when = None

                if (when is not None) and (when <= now):
                    heapq.heappop(self._heap)
                    items.append(item)
                else:
                    break

            if self._heap:
                self._earliest, _ = self._heap[0]
            else:
                self._earliest = None

        self._put_many(items)

    def _put(self, item):
        """Put the given item on self._q."""
        self._q.append(item)
        with self._q_not_empty:
            self._q_not_empty.notify()

    def _put_many(self, items):
        """Put the given items on self._q."""
        count = len(items)
        self._q.extend(items)
        with self._q_not_empty:
            self._q_not_empty.notify(count)


# vim: et sw=4 sts=4

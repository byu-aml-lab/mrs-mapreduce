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

"""Mrs. Chore Queue and Peon Thread"""

import heapq
import os
import random
import sys
import threading
import time
import traceback

try:
    import queue
except ImportError:
    import Queue as queue

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
        self._q = queue.Queue()
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

        The options are the same as those provided by queue.Queue.get.
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
            except queue.Full:
                pass


# vim: et sw=4 sts=4

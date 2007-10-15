#!/usr/bin/env python

# TODO: if the cookie check fails too many times, we may want to shut down
# with an error or do something else to make sure that the operator knows.

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

COOKIE_LEN = 32

import threading

class Worker(threading.Thread):
    def __init__(self, mapper, reducer, partition, **kwds):
        threading.Thread.__init__(self, **kwds)
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

        self.mapper = mapper
        self.reducer = reducer
        self.partition = partition

        # TODO: Should we have a queue of work to be done rather than
        # to just store a single item?
        self._task = None
        self._cond = threading.Condition()
        self.active = False
        self.exception = None

    def start_map(self, taskid, input, outprefix, reduce_tasks):
        from mapreduce import MapTask
        success = False
        self._cond.acquire()
        if self._task is None:
            self._task = MapTask(taskid, self.mapper, self.partition, input,
                    outprefix, reduce_tasks)
            success = True
            self._cond.notify()
        self._cond.release()
        return success

    def run(self):
        while True:
            self._cond.acquire()
            while self._task is None:
                print "About to wait in worker:"
                self._cond.wait()
                print "Returned from wait."
            task = self._task
            print "task:", task
            self._cond.release()

            print "About to run a new task."
            task.run()

            self._cond.acquire()
            self._task = None
            # TODO: notify server that we're done
            self._cond.release()

class CookieValidationError(Exception):
    pass

class SlaveRPC(object):
    # Be careful how you name your methods.  Any method not beginning with an
    # underscore will be exposed to remote hosts.

    def __init__(self, worker):
        self.alive = True
        self.worker = worker

        # Generate a cookie so that mostly only authorized people can connect.
        from random import choice
        import string
        possible = string.letters + string.digits
        self.cookie = ''.join(choice(possible) for i in xrange(COOKIE_LEN))

    def _listMethods(self):
        return SimpleXMLRPCServer.list_public_methods(self)

    def _check_cookie(self, cookie):
        if cookie != self.cookie:
            raise CookieValidationError

    def start_map(self, taskid, input, outprefix, reduce_tasks, cookie,
            **kwds):
        self._check_cookie(cookie)
        return self.worker.start_map(taskid, input, outprefix, reduce_tasks)

    def start_reduce(self, input, output, cookie, **kwds):
        self._check_cookie(cookie)
        return False

    def quit(self, cookie, **kwds):
        self._check_cookie(cookie)
        self.alive = False
        import sys
        print >>sys.stderr, "Quitting as requested by an RPC call."
        return True

    def ping(self, **kwds):
        """Master checking if we're still here.
        """
        return True


# vim: et sw=4 sts=4

#!/usr/bin/env python

# TODO: if the cookie check fails too many times, we may want to shut down
# with an error or do something else to make sure that the operator knows.

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
                self._cond.wait()
            task = self._task
            self._cond.release()

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

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

class Task(object):
    def __init__(self, function, input, output):
        self.function = function
        self.input = input
        self.output = output

class Worker(threading.Thread):
    def __init__(self, **kwds):
        threading.Thread.__init__(self, **kwds)
        # Die when all other non-daemon threads have exited:
        self.setDaemon(True)

        self._task = None
        self.active = False
        self.exception = None

    def run(self):
        pass

class CookieValidationError(Exception):
    pass

class SlaveRPC(object):
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

    def start_map(input, output, cookie):
        self._check_cookie(cookie)

    def start_reduce(input, output, cookie):
        self._check_cookie(cookie)

    def quit(cookie):
        self._check_cookie(cookie)
        self.alive = False
        import sys
        print >>sys.stderr, "Quitting as requested by an RPC call."
        return True

    def ping(self):
        """Master checking if we're still here.
        """
        return True


# vim: et sw=4 sts=4

#!/usr/bin/env python

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

import threading

class Task(object):
    def __init__(self, function, input, output):
        self.function = function
        self.input = input
        self.output = output

class Worker(threading.Thread):
    def __init__(self, **kwds):
        threading.Thread(self, **kwds)
        self._task = None

class SlaveRPC(object):
    def _listMethods(self):
        return SimpleXMLRPCServer.list_public_methods(self)

    def signin(self, cookie, host=None, port=None):
        """Slave reporting for duty.
        """
        print 'host: %s, port: %s' % (host, port)
        return 4

    def start_map(input, output, cookie):
        pass

    def start_reduce(input, output, cookie):
        pass

    def ping(self):
        """Master checking if we're still here.
        """
        return True



# vim: et sw=4 sts=4

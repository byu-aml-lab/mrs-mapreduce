#!/usr/bin/env python

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

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

import threading, SimpleXMLRPCServer

# TODO: Switch to Twisted(?). Note that the current implementation uses a
# synchronous server.  We'll probably want to change that at some point.  Look
# at: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/526625 and
# http://www.rexx.com/~dkuhlman/twisted_patterns.html#the-xml-rpc-pattern

# TODO: check for basic HTTP authentication?


class MrsXMLRPCRequestHandler(SimpleXMLRPCServer.SimpleXMLRPCRequestHandler):
    """Mrs Request Handler

    Your functions will get called with the additional keyword arguments host
    and port.  Make sure to do server.register_instance().  We don't support
    register_method because we're lazy.

    self.server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),
            requestHandler=MrsXMLRPCRequestHandler)

    If you don't specify requestHandler=MrsXMLRPCRequestHandler, then
    all of your functions will get called with host=None, port=None.

    We override _dispatch so that we can get the host and port.  Technically,
    being able to override _dispatch in SimpleXMLRPCRequestHandler is a
    deprecated feature, but we don't care.
    """
    def _dispatch(self, method, params):
        """Mrs Dispatch

        Your function will get called with the additional keyword arguments
        host and port.  Make sure to do server.register_instance().  We don't
        support register_method because we're lazy.
        """
        if method.startswith('_'):
            raise AttributeError('attempt to access private attribute "%s"' % i)
        else:
            function = getattr(self.server.instance, method)

        host, port = self.client_address
        kwds = dict(host=host, port=port)
        return function(*params, **kwds)


class RPCThread(threading.Thread):
    """Thread for listening for incoming connections from slaves.
    
    Listen on port 8000 for slaves to connect:
    server = RPCServer(8000)
    server.start()
    """
    def __init__(functions, port, **kwds):
        threading.Thread(self, **kwds)
        self.server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),
                requestHandler=MrsXMLRPCRequestHandler)
        #self.server.register_introspection_functions()
        self.server.register_instance(functions)

    def run():
        self.server.serve_forever()

class MasterRemoteFunctions(object):
    def _listMethods(self):
        return SimpleXMLRPCServer.list_public_methods(self)

    def signin(self, cookie, host=None, port=None):
        """Slave reporting for duty.
        """
        print 'host: %s, port: %s' % (host, port)
        return 4

    def ping(self):
        """Slave checking if we're still here.
        """
        return 4

if __name__ == '__main__':
    # Testing standalone server.
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', 8000),
            requestHandler=MrsXMLRPCRequestHandler)
    instance = MasterRemoteFunctions()
    server.register_instance(instance)
    server.serve_forever()

# vim: et sw=4 sts=4

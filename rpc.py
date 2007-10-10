#!/usr/bin/env python

import threading, SimpleXMLRPCServer

# TODO: Switch to Twisted(?). Note that the current implementation uses a
# synchronous server.  We'll probably want to change that at some point.  Look
# at: http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/526625 and
# http://www.rexx.com/~dkuhlman/twisted_patterns.html#the-xml-rpc-pattern

# TODO: check for basic HTTP authentication?

def new_server(functions, port):
    """Return an RPC server for incoming RPC requests.

    Note that functions is an "instance" to be given to SimpleXMLRPCServer.
    """
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port),
            requestHandler=MrsXMLRPCRequestHandler)
    #server.register_introspection_functions()
    server.register_instance(functions)
    return server

class RPCThread(threading.Thread):
    """Thread for listening for incoming RPC requests.
    
    Listen on port 8000 for slaves to connect:
    server = RPCServer(functions, 8000)
    server.start()

    Note that functions is an "instance" to be given to SimpleXMLRPCServer.
    """
    def __init__(self, functions, port, **kwds):
        threading.Thread(self, **kwds)
        self.server = new_server(functions, port)

    def run(self):
        self.server.serve_forever()


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


if __name__ == '__main__':
    # Testing standalone server.
    server = SimpleXMLRPCServer.SimpleXMLRPCServer(('localhost', 8000),
            requestHandler=MrsXMLRPCRequestHandler)
    instance = MasterRemoteFunctions()
    server.register_instance(instance)
    server.serve_forever()

# vim: et sw=4 sts=4

#!/usr/bin/env python

import threading

class Slave(object):
    def __init__(self, host, port, cookie):
        self.host = host
        self.port = port
        self.cookie = cookie

class Slaves(object):
    pass

class MasterRPC(object):
    def __init__(self):
        self.slaves = []

    def _listMethods(self):
        import SimpleXMLRPCServer
        return SimpleXMLRPCServer.list_public_methods(self)

    def signin(self, cookie, slave_port, host=None, port=None):
        """Slave reporting for duty.
        """
        slave = Slave(host, slave_port, cookie)
        self.slaves.append(slave)
        return True

    def ping(self):
        """Slave checking if we're still here.
        """
        return True


if __name__ == '__main__':
    # Testing standalone server.
    import rpc
    instance = MasterRPC()
    PORT = 8000
    #PORT = 0
    server = rpc.new_server(instance, host='127.0.0.1', port=PORT)
    server.serve_forever()


# vim: et sw=4 sts=4

#!/usr/bin/env python

class Slaves(object):
    pass

class MasterRPC(object):
    def _listMethods(self):
        return SimpleXMLRPCServer.list_public_methods(self)

    def signin(self, cookie, slave_port, host=None, port=None):
        """Slave reporting for duty.
        """
        return 4

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

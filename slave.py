#!/usr/bin/env python


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

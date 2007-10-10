#!/usr/bin/env python


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



# vim: et sw=4 sts=4

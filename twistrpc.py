# Mrs
# Copyright 2008-2009 Brigham Young University
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
# Inquiries regarding any further use of Mrs, please contact the Copyright
# Licensing Office, Brigham Young University, 3760 HBLL, Provo, UT 84602,
# (801) 422-9339 or 422-3821, e-mail copyright@byu.edu.

"""Mrs. Twist RPC

Modified variants of Twisted's RPC clients and servers.  We add the ability to
deal with timeouts in network code and also allow RPC calls to happen from
threads outside the reactor.  We also allow RPC methods on the server to
receive the client object.

Most Mrs code uses FromThreadProxy and RequestXMLRPC.
"""

DEFAULT_RETRIES = 3
DEFAULT_TIMEOUT = 30.0
RETRY_DELAY = 0.1

import xmlrpclib
from twisted.web import server, xmlrpc
from twisted.internet import reactor, defer, error
from twist import reactor_call, block

from logging import getLogger
logger = getLogger('mrs')


class TimeoutQueryFactory(xmlrpc._QueryFactory):
    """XMLRPC Query Factory that supports timeouts.

    We extend Twisted's QueryFactory to allow connections to timeout.  When a
    timeout occurs, we'll errback to the normal deferred.

    Twisted has this strange notion of a "factory" that isn't at all obvious
    (I think Twisted secretly wishes it was written in Java).  Anyway, a
    Twisted "factory" is what any normal person would call a "server".  They
    kind of make sense for a server, where you're spawning off multiple
    connections.  However, they're unfortunately used for clients, too, hence
    the meaningless name for a meaningless abstraction.

    Bitter feelings aside, let me try to explain what a "factory" is for a
    client.  In the normal case, it's just a meaningless layer that's only
    used for its buildProtocol method.  However, it can also be used to do
    fancy stuff like automatically reconnecting, in which case it creates a
    new protocol object for each connection.  However, even automatic
    reconnection isn't designed properly, so the factory abstraction really is
    quite broken for clients.
    """
    def __init__(self, *args, **kwds):
        self.failures = 0
        if 'timeout' in kwds:
            self.timeout = kwds['timeout']
            del kwds['timeout']
        else:
            self.timeout = None

        self.port = kwds['port']
        del kwds['port']

        self.secure = kwds['secure']
        del kwds['secure']

        self.retries = kwds['retries']
        del kwds['retries']

        self.connector = None
        self.canceled = False
        self.timed_out = False
        xmlrpc._QueryFactory.__init__(self, *args, **kwds)

    def connect(self):
        if self.secure:
            from twisted.internet import ssl
            self.connector = reactor.connectSSL(self.host, self.port or 443,
                    self, ssl.ClientContextFactory())
        else:
            self.connector = reactor.connectTCP(self.host, self.port or 80,
                    self)

    def cancel(self):
        if self.connector:
            self.canceled = True
            self.connector.disconnect()
        else:
            raise RuntimeError('Cancel called on a factory with no connector.')

    def buildProtocol(self, addr):
        p = xmlrpc._QueryFactory.buildProtocol(self, addr)
        if self.timeout:
            self._timeout_call = reactor.callLater(self.timeout,
                    self._timeout_func, p)
            self.deferred.addBoth(self._cancel_timeout)
        return p

    def _timeout_func(self, p):
        """Called when a timeout occurs."""
        self.timed_out = True
        p.transport.loseConnection()
        err = defer.TimeoutError()
        self.deferred.errback(err)

    def _cancel_timeout(self, result):
        """Called when the deferred is done (either succeeded or failed)"""
        if self._timeout_call.active():
            self._timeout_call.cancel()
        return result

    # We override this so the deferred doesn't get 2 errbacks calls:
    def clientConnectionLost(self, connector, err):
        if self.canceled:
            pass
        elif self.timed_out:
            pass
        elif (self.failures < self.retries and
                err.check(error.DNSLookupError, error.ConnectError)):
            logger.info('Retrying an RPC call.')
            self.failures += 1
            self.connect()
        else:
            xmlrpc._QueryFactory.clientConnectionLost(self, connector, err)

    clientConnectionFailed = clientConnectionLost


class TimeoutProxy(xmlrpc.Proxy):
    """XMLRPC Proxy that supports timeouts.

    We extend Twisted's Proxy to allow connections to timeout.  When a timeout
    occurs, we'll errback to the normal deferred.
    """
    queryFactory = TimeoutQueryFactory

    def __init__(self, url, timeout=DEFAULT_TIMEOUT, retries=DEFAULT_RETRIES,
            **kwds):
        self.timeout = timeout
        self.retries = retries

        cleaned_url = rpc_url(url)
        xmlrpc.Proxy.__init__(self, cleaned_url, **kwds)

    def callRemote(self, *args):
        """Overrides callRemote to always go through powerful_call."""
        factory = self.powerful_call(*args)
        return factory.deferred

    # ripped almost exactly from twisted.web.xmlrpc:
    def powerful_call(self, method, *args):
        """Call a remote RPC method and return a deferred and a connector.

        This is almost the same as callRemote in twisted.web.xmlrpc, but
        it's much more powerful because you have more control over the
        connection.
        """
        factory = self.queryFactory(
            self.path, self.host, method, self.user,
            self.password, self.allowNone, args, timeout=self.timeout,
            port=self.port, secure=self.secure, retries=self.retries)
        factory.connect()
        return factory

    def blocking_call(self, *args):
        """Make a blocking XML RPC call to a remote server.
        
        This can be called from another thread.
        """
        # pause between 'blocking call' and 'calling'
        deferred = self.deferred_call(*args)
        result = block(deferred)
        return result

    def deferred_call(self, *args):
        """Make a deferred XML RPC call to a remote server.
        
        This can be called from another thread.
        """
        deferred = reactor_call(self.callRemote, *args)
        return deferred


def rpc_url(urlstring):
    """Tidy a URL to be used to connect to an XML RPC server.

    >>> rpc_url('http://localhost')
    'http://localhost/RPC2'
    >>> rpc_url('http://localhost/')
    'http://localhost/RPC2'
    >>> rpc_url('http://localhost/path/to/xmlrpc')
    'http://localhost/path/to/xmlrpc'
    >>> rpc_url('localhost/path/to/xmlrpc')
    'http://localhost/path/to/xmlrpc'
    >>>
    """
    from urlparse import urlsplit, urlunsplit

    if '://' not in urlstring:
        urlstring = 'http://' + urlstring

    scheme, netloc, path, query, fragment = urlsplit(urlstring)
    if not path and not query and not fragment:
        path = '/RPC2'
    return urlunsplit((scheme, netloc, path, query, fragment))


class RequestXMLRPC(xmlrpc.XMLRPC):
    """Extension of XMLRPC which passes the client to RPC methods."""

    # We redefine the render function to send in the named parameters.
    def render(self, request):
        request.content.seek(0, 0)
        args, functionPath = xmlrpclib.loads(request.content.read())
        try:
            function = self._getFunction(functionPath)
        except Fault, f:
            self._cbRender(f, request)
        else:
            request.setHeader("content-type", "text/xml")
            if hasattr(function, "uses_request"):
                args = (request,) + args
            defer.maybeDeferred(function, *args).addErrback(
                self._ebRender
            ).addCallback(
                self._cbRender, request
            )
        return server.NOT_DONE_YET


def uses_request(f):
    """Decorate f with the attribute `uses_request`.

    When XMLRPC renders the given XML RPC method, it will pass the Request
    as the first argument.
    """
    f.uses_request = True
    return f


# vim: et sw=4 sts=4

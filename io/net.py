#!/usr/bin/env python

# TODO: when twisted.web2 comes out, we should switch to use it (twisted.web
# is a bit primitive)

from twisted.web.client import HTTPDownloader, HTTPClientFactory
from twisted.internet import defer, reactor

def download(url, buf):
    """Download from url to a Mrs Buffer

    Incoming data are appended to buf, but care is taken to preserve the
    position in the file.  This means that another function in the same thread
    can be reading data from the file without being disrupted.  Note, however,
    that a function in another thread would need its own file handle since
    this cooperative access to the file is not threadsafe.

    Note that download returns a Twisted Deferred.  Each time new data are
    added to the file, the Deferred is called (technically, it's a copy of the
    Deferred).  The paramater to the callback is a boolean indicating if end
    of file has been reached.

    >>> from buffer import Buffer
    >>> import sys
    >>>

    We'll be downloading the New Testament as a test (this will definitely
    download in more than one chunck).
    >>> url = 'http://www.gutenberg.org/dirs/etext05/bib4010h.htm'
    >>>

    Create a Mrs Buffer to download into:
    >>> buf = Buffer()
    >>>

    >>> deferred = download(url, buf)
    >>> callback = TestingCallback()
    >>> tmp = deferred.addCallback(callback)
    >>> reactor.run()
    >>>

    Make sure the file finished downloading and came in multiple chunks:
    >>> callback.saw_eof
    True
    >>> callback.count > 1
    True
    >>> print >>sys.stderr, "FYI: count when downloading N.T.:", callback.count
    >>>

    Make sure that the data were read correctly:
    >>> lines = [buf.readline().rstrip() for i in xrange(20)]
    >>> print lines[16]
    <a href="#begin">THE PROJECT GUTENBERG BIBLE, King James,
    >>> print lines[17]
    <br>Book 40: Matthew</a>
    >>>
    """

    factory = HTTPReader(url, buf)

    from urlparse import urlparse
    u = urlparse(url)
    port = u.port
    if not port:
        if u.scheme == 'http':
            port = 80
        elif u.scheme == 'https':
            port = 443

    # Connect
    if u.scheme == 'https':
        from twisted.internet import ssl
        contextFactory = ssl.ClientContextFactory()
        reactor.connectSSL(u.hostname, port, factory, contextFactory)
    else:
        reactor.connectTCP(u.hostname, port, factory)

    return factory.deferred

class TestingCallback(object):
    def __init__(self):
        self.count = 0
        self.saw_eof = False

    def __call__(self, eof):
        if eof:
            self.saw_eof = True
            reactor.stop()
        else:
            self.count += 1

def test_download():
    import doctest
    doctest.testmod()


class HTTPReader(HTTPDownloader):
    """Twisted protocol for downloading to a Mrs Buffer

    Each time new data are added to the buffer, a copy of the deferred is
    called.  When downloading completes, the original deferred is finally
    called.
    """
    def __init__(self, url, buf, method='GET', postdata=None,
            headers=None):
        self.requestedPartial = 0
        HTTPClientFactory.__init__(self, url, method=method,
                postdata=postdata, headers=headers, agent='Mrs')
        self.deferred = defer.Deferred()
        self.waiting = 1

        self.buf = buf

    def pageStart(self, partialContent):
        assert(not partialContent or self.requestedPartial)
        if self.waiting:
            self.waiting = 0

    def pagePart(self, data):
        self.buf.append(data)

        # Twisted won't let us pass a new Deferred to a Deferred, like so:
        ##olddef.callback(self.deferred)
        # So instead, we callback a shallow copy of the Deferred:
        newdef = defer.Deferred()
        newdef.callbacks = list(self.deferred.callbacks)
        newdef.callback(False)

    def pageEnd(self):
        self.deferred.callback(True)


if __name__ == '__main__':
    test_download()

# vim: et sw=4 sts=4

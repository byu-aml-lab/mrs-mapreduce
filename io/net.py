#!/usr/bin/env python

# TODO: when twisted.web2 comes out, we should switch to use it (twisted.web
# is a bit primitive)

from twisted.web.client import HTTPDownloader, HTTPClientFactory
from twisted.internet import defer, reactor

class HTTPReader(HTTPDownloader):
    def __init__(self, url, bucket, method='GET', postdata=None,
            headers=None):
        self.requestedPartial = 0
        HTTPClientFactory.__init__(self, url, method=method,
                postdata=postdata, headers=headers, agent='Mrs')
        self.deferred = defer.Deferred()
        self.waiting = 1

        self.bucket = bucket

    def pageStart(self, partialContent):
        assert(not partialContent or self.requestedPartial)
        if self.waiting:
            self.waiting = 0

    def pagePart(self, data):
        self.bucket.raw_data(data)

    def pageEnd(self):
        self.bucket.raw_eof()
        self.deferred.callback(None)


def download(url, bucket):
    """Download from url to bucket.

    As data arrive, bucket.raw_data(data) will be called.
    """
    factory = HTTPReader(url, bucket)

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


if __name__ == '__main__':
    from mrs.datasets import Bucket
    buck = Bucket()
    deferred = download('http://www.mcnabbs.org/', buck)
    deferred.addCallback(lambda value: reactor.stop())
    reactor.run()
    print 'goodbye'

# vim: et sw=4 sts=4

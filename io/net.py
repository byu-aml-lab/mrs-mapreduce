#!/usr/bin/env python

# TODO: when twisted.web2 comes out, we should switch to use it (twisted.web
# is a bit primitive)

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

# Mrs
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

import twisttest
from textformat import TextReader, TextWriter
from hexformat import HexReader, HexWriter, hexformat_sort

reader_map = {
        'txt': TextReader,
        'mrsx': HexReader,
        }
writer_map = {
        'txt': TextWriter,
        'mrsx': HexWriter,
        }
default_format = TextReader

def writerformat(extension):
    return writer_map[extension]

# TODO: Find a better way to infer the file format.
def fileformat(filename):
    """Guess the file format according to extension of the given filename."""
    import os
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_format)

def openreader(url):
    """Open a url or file and wrap an input format around it.
    """
    buf = openbuf(url)
    format = fileformat(url)
    return format(buf)

def urlconsume(url, consumer):
    """Open a url and start streaming its contents to a consumer.

    The url is assumed to be a file if no scheme is given.  We return a
    deferred that will be called back when streaming is complete.


    We'll be downloading the New Testament as a test (this will definitely
    download in more than one chunck).
    >>> url = 'http://www.gutenberg.org/dirs/etext05/bib4010h.htm'
    >>> consumer = twisttest.TestConsumer()
    >>> deferred = urlconsume(url, consumer)
    >>> d = deferred.addBoth(twisttest.pause_reactor)
    >>> twisttest.resume_reactor()
    >>>

    Make sure that the data were read correctly:
    >>> lines = consumer.buffer.splitlines()
    >>> print lines[16]
    <a href="#begin">THE PROJECT GUTENBERG BIBLE, King James,
    >>> print lines[17]
    <br>Book 40: Matthew</a>
    >>>
    """
    from producers import FileProducer, HTTPClientProducerFactory
    import urlparse, urllib2

    u = urlparse.urlsplit(url, 'file')
    if u.scheme == 'file':
        producer = FileProducer(u.path)
        deferred = producer.deferred
    elif u.scheme in ('http', 'https'):
        from twisted.internet import reactor
        factory = HTTPClientProducerFactory(url, consumer)
        port = u.port

        if u.scheme == 'http':
            if not port:
                port = 80
            reactor.connectTCP(u.hostname, port, factory)
        else:
            if not port:
                port = 443
            from twisted.internet import ssl
            contextFactory = ssl.ClientContextFactory()
            reactor.connectSSL(u.hostname, port, factory, contextFactory)

        deferred = factory.deferred
    else:
        raise RuntimeError("Unsupported URL scheme: %s" % u.scheme)

    return deferred


def test():
    import doctest
    twisttest.start_reactor()
    doctest.testmod()
    twisttest.cleanup_reactor()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

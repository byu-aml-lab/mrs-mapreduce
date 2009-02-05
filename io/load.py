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
from consumer import LineConsumer
from textformat import TextWriter
from hexformat import HexConsumer, HexWriter, hexformat_sort

reader_map = {
        #'mtxt': TextConsumer,
        'mrsx': HexConsumer,
        }
writer_map = {
        'mtxt': TextWriter,
        'mrsx': HexWriter,
        }
default_format = LineConsumer


def writerformat(extension):
    """Returns the writer class associated with the given file extension."""
    return writer_map[extension]


def fileformat(filename):
    """Returns the Consumer class associated with the given file extension."""
    import os
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_format)


def open_url(url):
    """Opens a url or file and returns a filelike object."""
    import urlparse, urllib2
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path)
    else:
        f = urllib2.urlopen(url)
    return f


def blocking_fill(url, bucket):
    """Open a url or file and write it to a bucket."""
    from producers import SerialProducer
    consumer_cls = fileformat(url)
    consumer = consumer_cls(bucket)
    producer = SerialProducer(url, consumer)
    producer.run()


def fillbucket(url, bucket, blockingthread):
    """Open a url or file and start writing it to a bucket.

    This will return a deferred that will be called back when the writing
    is completed.  The blockingthread is an instance of BlockingThread that
    will be used if IO cannot be handled natively in Twisted.
    """
    consumer_cls = fileformat(url)
    consumer = consumer_cls(bucket)
    deferred = urlconsume(url, consumer, blockingthread)
    return deferred


def urlconsume(url, consumer, blockingthread):
    """Open a url and start streaming its contents to a consumer.

    The url is assumed to be a file if no scheme is given.  We return a
    deferred that will be called back when streaming is complete.  The
    blockingthread is an instance of BlockingThread that will be used if IO
    cannot be handled natively in Twisted.  The blockingthread does not need
    to be started before calling urlconsume.


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
    from producers import URLProducer, HTTPClientProducerFactory
    import urlparse, urllib2

    u = urlparse.urlsplit(url, 'file')
    if u.scheme in ('http', 'https'):
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
        producer = URLProducer(u.path, consumer, blockingthread)
        blockingthread.register(producer)
        if not blockingthread.isAlive():
            blockingthread.start()
        deferred = producer.deferred

    return deferred


def test():
    import doctest
    twisttest.start_reactor()
    doctest.testmod()
    twisttest.cleanup_reactor()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

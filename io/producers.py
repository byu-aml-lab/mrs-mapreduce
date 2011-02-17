# Mrs
# Copyright 2008-2011 Brigham Young University
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

# If BLOCKSIZE is too big, then latency will be high; if it's too low, then
# throughput will be low.
BLOCKSIZE = 1000

TEST_HOST = 'cs.byu.edu'

from twisted.web.client import HTTPClientFactory, HTTPPageDownloader
from twisted.internet import defer, reactor, abstract, interfaces, main
from zope.interface import implements
import twisttest


# TODO: consider reading chunks of data instead of reading and writing all of
# it at once (which can take a long time).
class URLProducer(object):
    """A Blocking Producer which reads data from any URL.

    For the producer to actually start producing, it needs to be registered
    with a running BlockingThread.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, url, consumer, blocking_thread):
        self.url = url
        self.blocking_thread = blocking_thread
        self.deferred = defer.Deferred()

        consumer.registerProducer(self, streaming=True)
        self.consumer = consumer

    def run(self):
        """Loads data and sends it to the consumer.
        
        Called by the BlockingThread.
        """
        from load import open_url
        f = open_url(self.url)
        data = f.read()
        f.close()
        reactor.callFromThread(self._write, data)

    def _write(self, data):
        """Sends data to the consumer.

        Runs within the reactor thread.
        """
        self.consumer.write(data)
        self.consumer.unregisterProducer()
        self.deferred.callback(None)


class SerialProducer(object):
    """A Producer which reads data from any URL.

    The producer will do all work when the run method is called, and it does
    not interact with the reactor in any way.  This producer should only be
    used in sequential code or in a non-reactor thread.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, url, consumer):
        self.url = url
        consumer.registerProducer(self, streaming=True)
        self.consumer = consumer

    def run(self):
        """Loads data and sends it to the consumer.
        
        Called by the BlockingThread.
        """
        from load import open_url
        f = open_url(self.url)
        data = f.read()
        f.close()
        self.consumer.write(data)
        self.consumer.unregisterProducer()


class HTTPClientProducerProtocol(HTTPPageDownloader):
    """A varient of HTTPPageDownloader that lets you limit the buffer size."""

    def connectionMade(self):
        self.transport.bufferSize = self.factory.blocksize
        HTTPPageDownloader.connectionMade(self)


class HTTPClientProducerFactory(HTTPClientFactory):
    """Twisted protocol factory which serves as a Push Producer

    Set up the URL we will use for testing:
    >>> url = 'http://%s/' % (TEST_HOST)
    >>>


    >>> consumer = twisttest.TestConsumer()
    >>> factory = HTTPClientProducerFactory(url, consumer)
    >>> connector = reactor.connectTCP(TEST_HOST, 80, factory)
    >>> factory.blocksize = 100
    >>> d = factory.deferred.addBoth(twisttest.pause_reactor)
    >>> twisttest.resume_reactor()
    >>>

    After downloading completes, the TestConsumer should have killed the
    reactor.  So, at this point, all of the data from the file should be
    in consumer.buffer.  Now we just need to make sure that the buffer
    contains the correct contents.

    >>> import urllib
    >>> real_data = urllib.urlopen(url).read()
    >>> real_data == consumer.buffer
    True
    >>> open('file1', 'w').write(real_data)
    >>> open('file2', 'w').write(consumer.buffer)
    >>>
    """
    implements(interfaces.IPushProducer)

    blocksize = BLOCKSIZE
    protocol = HTTPClientProducerProtocol

    def __init__(self, url, consumer, **kwds):
        HTTPClientFactory.__init__(self, url, **kwds)

        self.in_progress = True
        self.consumer = consumer
        self.consumer.registerProducer(self, streaming=True)

    def buildProtocol(self, addr):
        import sys
        self.protocol_instance = HTTPClientFactory.buildProtocol(self, addr)
        return self.protocol_instance

    def pageStart(self, partialContent):
        """Called by the protocol instance when connection starts."""
        if self.waiting:
            self.waiting = 0

    def pagePart(self, data):
        """Called by the protocol instance when a piece of data arrives."""
        import sys
        self.consumer.write(data)

    def pageEnd(self):
        """Called by the protocol instance when downloading is complete."""
        import sys
        self.in_progress = False
        self.consumer.unregisterProducer()
        self.deferred.callback(None)

    def noPage(self, reason):
        """Called by the protocol instance when an error occurs."""
        import sys
        self.consumer.unregisterProducer()
        if self.in_progress:
            self.deferred.errback(reason)

    def pauseProducing(self):
        """Called to pause streaming to the consumer."""
        self.protocol_instance.transport.stopReading()

    def resumeProducing(self):
        """Called to unpause streaming to the consumer."""
        self.protocol_instance.transport.startReading()

    def stopProducing(self):
        """Called to ask the producer to die."""
        self.protocol_instance.transport.loseConnection()


def test():
    import doctest
    twisttest.start_reactor()
    doctest.testmod()
    twisttest.cleanup_reactor()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

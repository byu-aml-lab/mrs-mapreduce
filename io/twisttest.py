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

from twisted.internet import reactor, interfaces
from zope.interface import implements

class TestProducer(object):
    """Simple producer for tests.

    It sends the data in chunks of the specified size.
    """

    implements(interfaces.IPushProducer)

    def __init__(self, data, consumer, size=None):
        self.data = data
        self.size = size
        self.consumer = consumer
        self.consumer.registerProducer(self, streaming=True)

    def push(self):
        """Push the data."""
        if self.size:
            chunk = self.data[0:size]
            self.data = self.data[size:]
        else:
            chunk = self.data
            self.data = ''
        self.consumer.write(chunk)

    def pauseProducing(self):
        pass

    def resumeProducing(self):
        pass

    def stopProducing(self):
        pass


class TestConsumer(object):
    """Simple consumer for doctests."""

    implements(interfaces.IConsumer)

    def __init__(self):
        self.buffer = ''

    def registerProducer(self, producer, streaming):
        self.producer = producer
        self.streaming = streaming

    def unregisterProducer(self):
        self.producer = None

    def write(self, data):
        self.buffer += data
        if not self.streaming:
            self.producer.resumeProducing()


class TestBucket(object):
    """Simple test class that looks like a Bucket."""
    def __init__(self):
        self.data = []

    def collect(self, itr):
        for k, v in itr:
            self.data.append((k, v))


def pause_reactor(value):
    """A callback function that temporarily stops the Twisted reactor.
    
    Used for doctests.
    """
    #print 'temporarily stopping the reactor'
    reactor.running = False
    return value

def resume_reactor():
    """Unpause the reactor.

    Used for doctests.
    """
    reactor.running = True
    reactor.mainLoop()

def start_reactor():
    """Setup the reactor prior to running doctests.

    Call this just before doctest.testmod().
    """
    reactor.startRunning()
    reactor.running = False

def cleanup_reactor():
    """Cleanup the reactor after running doctests.

    Call this just after doctest.testmod().
    """
    reactor.running = True
    reactor.stop()
    reactor.mainLoop()


# vim: et sw=4 sts=4

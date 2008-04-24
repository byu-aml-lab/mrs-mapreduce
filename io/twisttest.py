# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
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

from twisted.internet import reactor, interfaces
from zope.interface import implements


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

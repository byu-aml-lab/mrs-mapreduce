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

from twisted.internet import defer, reactor

# TODO: make Buffer test with a StringIO, so we can make better tests.

class Buffer(object):
    """Read data from a filelike object without blocking

    The purpose of the Buffer is to shield higher-level code from blocking I/O
    issues.  While doing so, it unifies network I/O and file I/O from the
    perspective of higher level code.  If data aren't available, then
    readline() simply returns None.
    
    Note that Buffer has an attribute named deferred, which is a Twisted
    Deferred.  Each time new data are added to the Buffer, it makes a copy of
    the deferred and performs a callback.  The paramater to the callback is a
    boolean indicating if end of file has been reached.

    Create a Mrs Buffer to download into:
    >>> buf = Buffer(open('/etc/passwd'))
    >>> deferred = buf.deferred
    >>> callback = TestingCallback()
    >>> tmp = deferred.addCallback(callback)
    >>> reactor.run()
    >>>

    Make sure the file finished downloading and came in multiple chunks:
    >>> callback.saw_eof
    True
    >>>
    """
    def __init__(self, filelike=None):
        self._data = ''
        self.filelike = filelike
        self.eof = False
        # Twisted Deferred (for callbacks)
        self.deferred = defer.Deferred()

        if filelike:
            # Register with the Twisted reactor
            reactor.addReader(self)
        self.filelike = filelike

    def _append(self, newdata):
        """Append data to the buffer.

        This is low-level; users should call doRead() or append() instead.
        Anyway, this writes to the low-level buffer and checks for eof.  Note
        that filelikes are automatically closed by Twisted.
        """
        assert(self.eof is False)
        if newdata == '':
            self.eof = True
            reactor.removeReader(self)
            self.deferred.callback(True)
        else:
            self._data += newdata

            # Twisted won't let us pass a new Deferred to a Deferred, like so:
            ##olddef.callback(self.deferred)
            # So instead, we callback a shallow copy of the
            # Deferred:
            newdef = defer.Deferred()
            newdef.callbacks = list(self.deferred.callbacks)
            newdef.callback(False)

    def doRead(self):
        """Called when data are available for reading

        To avoid blocking, read() will only be called once on the underlying
        filelike object.
        """
        assert(self.filelike is not None)
        newdata = self.filelike.read()
        self._append(newdata)

    def append(self, newdata):
        """Append additional data to the buffer
        """
        assert(self.filelike is None)
        self._append(newdata)

    def readline(self):
        """Read a complete line from the buffer

        Only complete lines are returned.  If no data are available, or if
        there is no newline character, None will be returned, and any
        remaining data will remain in the buffer.
        """
        data = self._data
        pos = data.find('\n')
        if pos is not -1:
            line = data[0:pos+1]
            self._data = data[pos+1:]
            return line
        else:
            return None

    def fileno(self):
        """Return the filenumber of the underlying filelike

        This will obviously fail if filelike is None or has no fileno.

        >>> b = Buffer(open('/etc/passwd'))
        >>> b.fileno() > 2
        True
        >>>
        """
        return self.filelike.fileno()

    def logPrefix(self):
        """Twisted really wants this method to exist."""
        return 'Mrs.Buffer'

    def connectionLost(self, reason):
        """Twisted really wants this method to exist."""
        import sys
        print >>sys.stderr, 'buffer.connectionLost was called!'
        print >>sys.stderr, 'Reason:', reason


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


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

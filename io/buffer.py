#!/usr/bin/env python

from twisted.internet import defer, reactor

# TODO: make Buffer test with a StringIO, so we can make better tests.

class Buffer(object):
    """Read data from a filelike object without blocking

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

    def close(self):
        if self.filelike:
            reactor.removeReader(self)
            self.filelike.close()

    def _append(self, newdata):
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

    def connectionLost(self):
        """Twisted really wants this method to exist."""
        print 'buffer.connectionLost was called!'


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

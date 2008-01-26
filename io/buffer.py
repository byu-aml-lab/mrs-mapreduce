#!/usr/bin/env python

class Buffer(object):
    """Read data from a filelike object without blocking

    """
    def __init__(self, filelike=None):
        self._data = ''
        self.filelike = filelike
        self.eof = False

    def _append(self, newdata):
        assert(self.eof is False)
        if newdata == '':
            self.eof = True
        else:
            self._data += newdata

    def doRead(self):
        """Called when data are available for reading

        To avoid blocking, read() will only be called once on the underlying
        filelike object.
        """
        assert(self.filelike is not None)
        newdata = self.filelike.read()

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


def test_buffer():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test_buffer()

# vim: et sw=4 sts=4

#!/usr/bin/env python
from itertools import islice


class TextFormat(object):
    """A basic line-oriented format, primarily for user interaction

    Initialize with a Mrs Buffer.  For reading, the key is the line number,
    and the value is the contents of the line.  For writing, the key and value
    are separated by spaces, with one entry per line.

    Note that we expose the deferred of the underlying Buffer object.

    Create a file-like object to play around with:
    >>> from cStringIO import StringIO
    >>> infile = StringIO("First Line.\\nSecond Line.\\nText without newline.")
    >>>

    Create a buffer from this file-like object and read into the buffer:
    >>> from buffer import Buffer
    >>> buf = Buffer(infile)
    >>> buf.doRead()
    >>>

    Create a TextFormat to read from the Buffer
    >>> text = TextFormat(buf)
    >>> text.readpair()
    (0, 'First Line.\\n')
    >>> text.readpair()
    (1, 'Second Line.\\n')
    >>> text.readpair()
    >>>
    """
    def __init__(self, buf):
        self.buf = buf
        self.lineno = 0
        self.deferred = buf.deferred

    def __iter__(self):
        return self

    def readpair(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        line = self.buf.readline()
        if line is not None:
            value = (self.lineno, line)
            self.lineno += 1
            return value
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.buf.readline()
        if line is '':
            raise StopIteration
        else:
            return (self.offset, line)

    def write(self, key, value):
        print >>self.file, key, value

    def close(self):
        self.file.close()


def test_textformat():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test_textformat()

# vim: et sw=4 sts=4

#!/usr/bin/env python
from itertools import islice


class TextFormat(object):
    """A basic line-oriented file format, primarily for user interaction

    Initialize with a file object.  For reading, the key is the file offset,
    and the value is the contents of the line.  For writing, the key and value
    are separated by spaces, with one entry per line.

    >>> from cStringIO import StringIO
    >>> infile = StringIO("First Line.\\nSecond Line.\\nText without newline.")
    >>> text = TextFormat(infile)
    >>> text.readpair()
    (0, 'First Line.\\n')
    >>> text.readpair()
    (12, 'Second Line.\\n')
    >>> text.readpair()
    >>>
    """
    def __init__(self, textfile):
        self.file = textfile
        self.offset = 0
        self._buffer = ''

    def __iter__(self):
        return self

    def readline(self):
        """Try to read in a line from the underlying input file.

        If no full line is available, then seek back and return ''.
        """
        self.offset = self.file.tell()
        line = self.file.readline()
        if line is '':
            return ''
        elif line[-1] != '\n':
            self.file.seek(self.offset)
            return ''
        else:
            return line

    def readpair(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        line = self.readline()
        if line is '':
            return None
        else:
            return (self.offset, line)

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.readline()
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

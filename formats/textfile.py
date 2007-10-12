#!/usr/bin/env python
from itertools import islice

class TextFile(object):
    """A file format for user interaction.

    Initialize with a file object.  For reading, the key is the file offset,
    and the value is the contents of the line.  For writing, the key and value
    are separated by spaces, with one entry per line.
    """
    def __init__(self, textfile):
        self.file = textfile

    def read(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        offset = self.file.tell()
        line = self.file.readline()
        if line:
            return (offset, line)
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        offset = self.file.tell()
        line = self.file.next()
        return (offset, line)

    def write(self, key, value):
        print >>self.file, key, value

    def close(self):
        self.file.close()

# vim: et sw=4 sts=4

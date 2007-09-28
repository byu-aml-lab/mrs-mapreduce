#!/usr/bin/env python
from itertools import islice

class TextFile(object):
    """A read-only file that reads line by line.

    The key is the file offset, and the value is the contents of the line.
    """
    def __init__(self, filename, mode='r'):
        self.file = open(filename, mode)

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

    def close(self):
        self.file.close()

# vim: et sw=4 sts=4

#!/usr/bin/env python

from textfile import TextFile
from hexfile import HexFile, hexfile_sort

format_map = {
        '.txt': TextFile,
        '.hexfile': HexFile,
        }
default_format = TextFile

def fileformat(filename):
    """Guess the file format according to extension of the given filename."""
    import os
    extension = os.path.splitext(filename)[1]
    return format_map.get(extension, default_format)

def openfile(filename):
    """Open a file to be read as a sequence of key-value pairs.
    
    The file format is inferred from the filename.
    """
    input_format = fileformat(filename)
    return input_format(open(filename))


# TODO: Make Output cache output in memory if it's not so big that it needs
# to be written to disk.
class Output(object):
    """Manage output for a map or reduce task."""
    def __init__(self, partition, splits, directory=None):
        self.partition = partition
        self.splits = splits
        self.buckets = [[] for i in xrange(n)]

        if directory is None:
            from tempfile import mkdtemp
            self.directory = mkdtemp()
            self.temp = True
        else:
            self.directory = directory
            self.temp = False

    def close(self):
        if self.temp:
            import os
            os.removedirs(self.directory)

    def collect(self, itr):
        """Collect all of the key-value pairs from the given iterator."""
        if n == 1:
            append = self.buckets[0].append
            for key, value in itr:
                append((key, value))
        else:
            for key, value in itr:
                split = self.partition(key, self.splits)
                self.buckets[split].append((key, value))

    def path(self, index):
        """Return the path to the output split for the given index."""
        filename = "split_%s.hexfile" % i
        return os.path.join(self.directory, filename)

    def savetodisk(self):
        """Write out all of the key-value pairs to files."""
        for i, bucket in enumerate(self.buckets):
            f = io.HexFile(open(self.path(i), 'w'))
            for key, value in bucket:
                f.write(key, value)
            f.close()

__all__ = ['TextFile', 'HexFile', 'hexfile_sort', 'fileformat', 'openfile']

# vim: et sw=4 sts=4

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

def openfile(filename, mode='r'):
    """Open a file to be read as a sequence of key-value pairs.
    
    The file format is inferred from the filename.
    """
    input_format = fileformat(filename)
    return input_format(open(filename, mode))


# TODO: Make Output cache output in memory if it's not so big that it needs
# to be written to disk.
class Output(object):
    """Manage output for a map or reduce task."""
    def __init__(self, partition, n, directory=None, format='hexfile'):
        self.partition = partition
        self.n = n
        self.splits = [([], None) for i in xrange(n)]
        self.format = format

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
        if self.n == 1:
            bucket, filename = self.splits[0]
            append = bucket.append
            for key, value in itr:
                append((key, value))
            filename = self.path(0)
            self.splits[0] = bucket, filename
        else:
            for key, value in itr:
                split = self.partition(key, self.n)
                bucket, filename = self.splits[split]
                bucket.append((key, value))
                if filename is None:
                    self.splits[split] = bucket, self.path(split)

    def path(self, index):
        """Return the path to the output split for the given index."""
        filename = "split_%s.%s" % (index, self.format)
        import os
        return os.path.join(self.directory, filename)

    def filenames(self):
        return [filename for bucket, filename in self.splits]

    def savetodisk(self):
        """Write out all of the key-value pairs to files."""
        for bucket, filename in self.splits:
            if filename is not None:
                f = openfile(filename, 'w')
                for key, value in bucket:
                    f.write(key, value)
                f.close()

__all__ = ['TextFile', 'HexFile', 'hexfile_sort', 'fileformat', 'openfile']

# vim: et sw=4 sts=4

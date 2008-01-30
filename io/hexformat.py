#!/usr/bin/env python
from textformat import TextReader, TextWriter
from itertools import islice

# TODO: sort should just sort on the first field

class HexReader(TextReader):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a Mrs Buffer.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.

    Create a file-like object to play around with:
    >>> from cStringIO import StringIO
    >>> infile = StringIO("4b6579 56616c7565\\n")
    >>>

    Create a buffer from this file-like object and read into the buffer:
    >>> from buffer import Buffer
    >>> buf = Buffer(infile)
    >>> buf.doRead()
    >>>

    >>> hex = HexReader(buf)
    >>> hex.readpair()
    ('Key', 'Value')
    >>> hex.readpair()
    >>>
    """
    def __init__(self, buf):
        super(HexReader, self).__init__(buf)

    def readpair(self):
        """Return the next key-value pair from the HexFormat or None if EOF."""
        line = self.buf.readline()
        if line:
            key, value = [dehex(field) for field in line.split()]
            return (key, value)
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.buf.readline()
        if line is None:
            raise StopIteration
        else:
            key, value = [dehex(field) for field in line.split()]
            return (key, value)


class HexWriter(TextWriter):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a file object.  The ASCII hexadecimal encoding of keys has
    the property that sorting the file will preserve the sort order.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """
    ext = 'mrsx'

    def __init__(self, file):
        super(HexWriter, self).__init__(file)

    def writepair(self, key, value):
        """Write a key-value pair to a HexFormat."""
        print >>self.file, enhex(key), enhex(value)

    def close(self):
        self.file.close()


def hexformat_sort(in_filenames, out_filename):
    """Sort one or more HexFormats into a new HexFormat.
    
    The ASCII hexadecimal encoding of keys has the property that sorting the
    file (with Unix sort) will preserve the sort order.

    Note that in_filenames can be the name of a single file or a list of names
    of files.
    """
    from subprocess import Popen
    # We give -s, which specifies a stable sort (only sort on the given key),
    # which empirically seems to be faster.
    args = ['sort', '-s', '-k1,1', '-o', out_filename]
    if isinstance(in_filenames, str):
        args.append(in_filenames)
    else:
        args += in_filenames
    proc = Popen(args)
    retcode = proc.wait()
    if retcode != 0:
        raise RuntimeError("Sort failed.")

def enhex(byteseq):
    """Encode an arbitrary byte sequence as an ASCII hexadecimal string.
    
    Make sure that whatever you send in is packed as a str.  Use the
    struct module in the standard Python library to help you do this.
    If you fail to do so, this will raise a TypeError.
    """
    # Note that hex() returns strings like '0x61', and we don't want the 0x.
    return ''.join(hex(ord(byte))[2:] for byte in byteseq)

def dehex(hexstr):
    """Decode a string of ASCII hexadecimal characters as a byte sequence.
    
    This will raise a ValueError if the input can't be interpreted as a string
    of hexadecimal characters (e.g., if you have a 'q' in there somewhere).
    By the way, you may wish to unpack the data.  Use the struct module to do
    this.
    """
    return ''.join(chr(int(pair, 16)) for pair in group_by_two(hexstr))

def group_by_two(s):
    """Read a string two characters at a time.

    If there's an odd number of characters, throw out the last one.
    """
    I = iter(s)
    while True:
        yield I.next() + I.next()


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()


# vim: et sw=4 sts=4

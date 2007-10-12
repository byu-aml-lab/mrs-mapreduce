#!/usr/bin/env python
from itertools import islice

# TODO: sort should just sort on the first field

class HexFile(object):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a file object.  The ASCII hexadecimal encoding of keys has
    the property that sorting the file will preserve the sort order.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """
    def __init__(self, hexfile):
        self.file = hexfile

    def read(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        line = self.file.readline()
        if line:
            key, value = [dehex(field) for field in line.split()]
            return (key, value)
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.file.next()
        key, value = [dehex(field) for field in line.split()]
        return (key, value)

    def write(self, key, value):
        """Write a key-value pair to a HexFile."""
        print >>self.file, enhex(key), enhex(value)

    def close(self):
        self.file.close()

def hexfile_sort(in_filenames, out_filename):
    """Sort one or more HexFiles into a new HexFile.
    
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

# vim: et sw=4 sts=4

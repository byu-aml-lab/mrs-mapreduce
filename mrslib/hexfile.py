#!/usr/bin/env python
from itertools import islice

class HexFile(object):
    """A key-value store using ASCII hexadecimal encoding

    This has the property that sorting the file will preserve the sort order.
    """
    def __init__(self, filename, mode='r'):
        self.file = open(filename, mode)

    def read(self):
        """Return the next key-value pair from the HexFile."""
        line = self.file.readline()
        key, value = [dehex(field) for field in line.split()]
        return (key, value)

    def write(self, key, value):
        """Write a key-value pair to a HexFile."""
        print >>self.file, enhex(key), enhex(value)

    def close(self):
        self.file.close()

def enhex(byteseq):
    """Encode an arbitrary byte sequence as an ASCII hexadecimal string.
    
    Make sure that whatever you send in is packed as a str.  Use the
    struct module in the standard Python library to help you do this.
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

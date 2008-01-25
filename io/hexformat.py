#!/usr/bin/env python
# Copyright 2008 Brigham Young University
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# Mrs.  If not, see <http://www.gnu.org/licenses/>.
#
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

from itertools import islice

# TODO: sort should just sort on the first field

class HexFormat(object):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a file object.  The ASCII hexadecimal encoding of keys has
    the property that sorting the file will preserve the sort order.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """
    def __init__(self, hexfile):
        self.file = hexfile

    def __iter__(self):
        return self

    def read(self):
        """Return the next key-value pair from the HexFormat or None if EOF."""
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
        """Write a key-value pair to a HexFormat."""
        print >>self.file, enhex(key), enhex(value)

    def close(self):
        self.file.close()


def hexformat_sort(in_filenames, out_filename):
    """Sort one or more HexFormats into a new HexFormat.
    
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

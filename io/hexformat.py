# Mrs
# Copyright 2008-2011 Brigham Young University
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
# Inquiries regarding any further use of Mrs, please contact the Copyright
# Licensing Office, Brigham Young University, 3760 HBLL, Provo, UT 84602,
# (801) 422-9339 or 422-3821, e-mail copyright@byu.edu.

import codecs

from consumer import LineConsumer
from textformat import TextWriter
from itertools import islice

# TODO: sort should just sort on the first field

hex_encoder = codecs.getencoder('hex_codec')
hex_decoder = codecs.getdecoder('hex_codec')

class HexConsumer(LineConsumer):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a Mrs Buffer.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """

    def __iter__(self):
        """Iterate over key-value pairs."""
        for line in self.lines():
            encoded_key, encoded_value = line.split()
            key, length = hex_decoder(encoded_key)
            value, length = hex_decoder(encoded_value)
            yield (key, value)


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

    def writepair(self, kvpair):
        """Write a key-value pair to a HexFormat."""
        key, value = kvpair
        encoded_key, length = hex_encoder(key)
        encoded_value, length = hex_encoder(value)
        print >>self.file, encoded_key, encoded_value


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


def test():
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

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
import gzip
from itertools import islice
import os
from six import b
from six import print_
import struct
import sys

try:
    from urllib.parse import urlparse
    from urllib.request import urlopen, URLopener
except ImportError:
    from urlparse import urlparse
    from urllib import URLopener
    from urllib2 import urlopen


DEFAULT_BUFFER_SIZE = 4096
# 1 is fast and unaggressive, 9 is slow and aggressive
COMPRESS_LEVEL = 9

hex_encoder = codecs.getencoder('hex_codec')
hex_decoder = codecs.getdecoder('hex_codec')


class Writer(object):
    """A writer takes a file-like object and writes key-value pairs.

    Writers do not flush or close the file object.

    This class is abstract.
    """
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def writepair(self, kvpair):
        raise NotImplementedError

    def finish(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.finish()


class Reader(object):
    """A reader takes a file-like object and iterates over key-value pairs.

    A Reader closes the file object if the close method is called or if it
    is used as a context manager (with the "with" statement).

    This class is abstract.
    """
    def __init__(self, fileobj):
        self.fileobj = fileobj

    def __iter__(self, kvpair):
        raise NotImplementedError

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.fileobj.close()


class LineReader(Reader):
    """Reads key-value pairs from a file object.

    In this basic reader, the key-value pair is composed of a line number
    and line contents.  Note that the most valuable method to override in this
    class is __iter__.
    """
    def __iter__(self):
        """Iterate over key-value pairs.

        Inheriting classes will almost certainly override this method.
        """
        for index, line in enumerate(self.fileobj):
            yield index, line


# TODO: implement TextReader
# class TextReader(LineReader) should read lines and do:
# key, value = line.split(None, 1)

class TextWriter(Writer):
    """A basic line-oriented format, primarily for user interaction

    For writing, the key and value are separated by spaces, with one entry per
    line.
    """
    ext = 'mtxt'

    def writepair(self, kvpair):
        key, value = kvpair
        print_(key, value, file=self.fileobj)


class HexReader(Reader):
    """A key-value store using ASCII hexadecimal encoding"""

    def __iter__(self):
        """Iterate over key-value pairs."""
        for line in self.fileobj:
            encoded_key, encoded_value = line.split()
            key, length = hex_decoder(encoded_key)
            value, length = hex_decoder(encoded_value)
            yield (key, value)


class HexWriter(TextWriter):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a file-like object.  The ASCII hexadecimal encoding of
    keys has the property that sorting the file will preserve the sort order.
    """
    ext = 'mrsx'

    def writepair(self, kvpair):
        """Write a key-value pair to a HexFormat."""
        key, value = kvpair
        encoded_key, length = hex_encoder(key)
        encoded_value, length = hex_encoder(value)
        print_(encoded_key, encoded_value, file=self.fileobj)


class BinWriter(TextWriter):
    """A key-value store using a simple compressed binary record format.

    By default, the given file will be closed when the writer is closed,
    but the close argument makes this configurable.  Setting close to False
    is useful for StringIO/BytesIO.
    """
    ext = 'mrsb'
    magic = b'MrsB'

    def __init__(self, fileobj, close=True):
        self.fileobj = fileobj
        self.fileobj.write(self.magic)

    def writepair(self, kvpair):
        """Write a key-value pair to a HexFormat."""
        key, value = kvpair

        binlen = struct.pack('<Q', len(key))
        self.fileobj.write(binlen)
        self.fileobj.write(key)

        binlen = struct.pack('<Q', len(value))
        self.fileobj.write(binlen)
        self.fileobj.write(value)


class BinReader(Reader):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a Mrs Buffer.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """
    magic = b'MrsB'

    def __init__(self, fileobj):
        self.fileobj = fileobj
        self._buffer = b''
        self._magic_read = False

    def __iter__(self):
        """Iterate over key-value pairs."""
        #TODO: check for 'MRSB' magic cookie
        if not self._magic_read:
            buf = self.fileobj.read(len(self.magic))
            if buf != self.magic:
                raise RuntimeError('Invalid file header: "%s"'
                    % buf.encode('hex_codec'))
            self._magic_read = True

        while True:
            key = self._read_record()
            if key is None:
                return
            value = self._read_record()
            if value is None:
                raise RuntimeError('File ended with a lone key')
            yield (key, value)

    def _fill_buffer(self, size=DEFAULT_BUFFER_SIZE):
        self._buffer += self.fileobj.read(size)

    def _read_record(self):
        LENFIELD_SIZE = 8
        if len(self._buffer) < LENFIELD_SIZE:
            self._fill_buffer()
        if not self._buffer:
            return None

        lenfield = self._buffer[:LENFIELD_SIZE]
        length, = struct.unpack('<Q', lenfield)

        end = LENFIELD_SIZE + length
        if end > len(self._buffer):
            self._fill_buffer(end - len(self._buffer))

        if end > len(self._buffer):
            raise RuntimeError('File ended unexpectedly')

        data = self._buffer[LENFIELD_SIZE:end]
        self._buffer = self._buffer[end:]
        return data


class ZipWriter(BinWriter):
    """A key-value store using a simple compressed binary record format.

    By default, the given fileobj will be closed when the writer is closed,
    but the close argument makes this configurable.  Setting close to False
    is useful for StringIO/BytesIO.
    """
    ext = 'mrsz'
    magic = b'MrsZ'

    def __init__(self, fileobj):
        fileobj = gzip.GzipFile(fileobj=fileobj, mode='wb',
                compresslevel=COMPRESS_LEVEL)
        super(ZipWriter, self).__init__(fileobj)

    def finish(self):
        # Close the gzip file (which does not close the underlying file).
        self.fileobj.close()


class ZipReader(BinReader):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a Mrs Buffer.

    TODO: we might as well base64-encode the value, rather than hex-encoding
    it, since it doesn't need to be sortable.
    """
    magic = b'MrsZ'

    def __init__(self, fileobj):
        self.original_file = fileobj
        fileobj = gzip.GzipFile(fileobj=fileobj, mode='rb')
        super(ZipReader, self).__init__(fileobj)

    def close(self):
        # Close the gzip file (which does not close the underlying file).
        self.fileobj.close()
        self.original_file.close()


def writerformat(extension):
    """Returns the writer class associated with the given file extension."""
    return writer_map[extension]


def fileformat(filename):
    """Returns the Reader class associated with the given file extension."""
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_read_format)


def open_url(url):
    """Opens a url or file and returns an appropriate key-value reader."""
    reader_cls = fileformat(url)

    parsed_url = urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path, 'rb')
    elif reader_cls is ZipReader and sys.version_info < (3, 2):
        # In Python <3.2, the gzip module is broken because it depends on the
        # underlying file being seekable (not true for url objects).
        opener = URLopener()
        filename, _ = opener.retrieve(url)
        f = open(filename, 'rb')
        os.unlink(filename)
    else:
        f = urlopen(url)

    return reader_cls(f)


def test():
    import doctest
    doctest.testmod()


reader_map = {
        'mrsx': HexReader,
        'mrsb': BinReader,
        'mrsz': ZipReader,
        }
writer_map = {
        'mtxt': TextWriter,
        'mrsx': HexWriter,
        'mrsb': BinWriter,
        'mrsz': ZipWriter,
        }
default_read_format = LineReader
default_write_format = BinWriter

# vim: et sw=4 sts=4

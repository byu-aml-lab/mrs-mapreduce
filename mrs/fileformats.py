# Mrs
# Copyright 2008-2012 Brigham Young University
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

from __future__ import division, print_function

import codecs
import functools
import gzip
from itertools import islice
import os
import struct
import sys

PY3 = sys.version_info[0] == 3
if PY3:
    from urllib.parse import urlparse
    from urllib.request import urlopen, URLopener
    import io
    import pickle
else:
    from urlparse import urlparse
    from urllib import URLopener
    from urllib2 import urlopen
    import cPickle as pickle

from . import hdfs


DEFAULT_BUFFER_SIZE = 4096
# 1 is fast and unaggressive, 9 is slow and aggressive
COMPRESS_LEVEL = 9

hex_encoder = codecs.getencoder('hex_codec')
hex_decoder = codecs.getdecoder('hex_codec')


class Writer(object):
    """A writer takes a file-like object and writes key-value pairs.

    Writers do not flush or close the file object.

    This class is abstract.

    Parameters:
        fileobj: A file or filelike object.
        serializers: A Serializers instance (such as a namedtuple) for
            serializing from Python objects to bytes.  If a serializer is
            None, use pickle.  Otherwise, use its `dumps` function.  A `dumps`
            function set to None indicates that the keys are already bytes.
    """
    def __init__(self, fileobj, serializers=None):
        self.fileobj = fileobj

        if serializers is None:
            key_s = None
            value_s = None
        else:
            key_s = serializers.key_s
            value_s = serializers.value_s

        if key_s is None:
            self.dumps_key = functools.partial(pickle.dumps, protocol=-1)
        else:
            self.dumps_key = key_s.dumps

        if value_s is None:
            self.dumps_value = functools.partial(pickle.dumps, protocol=-1)
        else:
            self.dumps_value = value_s.dumps

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

    Parameters:
        fileobj: A file or filelike object.
        serializers: A Serializers instance (such as a namedtuple) for
            serializing from Python objects to bytes.  If a serializer is
            None, use pickle.  Otherwise, use its `dumps` function.  A `dumps`
            function set to None indicates that the keys are already bytes.
    """
    def __init__(self, fileobj, serializers=None):
        self.fileobj = fileobj

        if serializers is None:
            key_s = None
            value_s = None
        else:
            key_s = serializers.key_s
            value_s = serializers.value_s

        if key_s is None:
            self.loads_key = pickle.loads
        else:
            self.loads_key = key_s.loads

        if value_s is None:
            self.loads_value = pickle.loads
        else:
            self.loads_value = value_s.loads

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

    In this basic reader, the key-value pair is composed of a line number and
    line contents (as a string).  The input file is assumed to be encoded in
    UTF-8, and the error mode is 'replace' (invalid characters are replaced
    with u'\ufffd').
    """
    def __init__(self, fileobj, *args, **kwds):
        if PY3:
            fileobj = io.TextIOWrapper(fileobj, encoding='utf-8',
                    errors='replace')
        super(LineReader, self).__init__(fileobj, *args, **kwds)

    if PY3:
        def __iter__(self):
            """Iterate over key-value pairs.

            Inheriting classes will almost certainly override this method.
            """
            return enumerate(self.fileobj)
    else:
        def __iter__(self):
            """Iterate over key-value pairs.

            Inheriting classes will almost certainly override this method.
            """
            for i, s in enumerate(self.fileobj):
                yield i, s.decode('utf-8', errors='replace')


class BytesLineReader(Reader):
    """Reads key-value pairs from a file object.

    In this basic reader, the key-value pair is composed of a line number
    and line contents (as a bytes object).
    """
    def __iter__(self):
        """Iterate over key-value pairs.

        Inheriting classes will almost certainly override this method.
        """
        return enumerate(self.fileobj)


# TODO: implement TextReader
# class TextReader(LineReader) should read lines and do:
# key, value = line.split(None, 1)

class TextWriter(Writer):
    """A basic line-oriented format, primarily for user interaction.

    The key and value are first converted to unicode and encoded with UTF-8
    and then written to the file separated by spaces, with one entry per line.
    """
    ext = 'mtxt'

    def __init__(self, fileobj, *args, **kwds):
        if PY3:
            fileobj = io.TextIOWrapper(fileobj, encoding='utf-8', newline='\n')
        super(TextWriter, self).__init__(fileobj, *args, **kwds)

    if PY3:
        def writepair(self, kvpair):
            key, value = kvpair
            self.fileobj.write(str(key))
            self.fileobj.write(' ')
            self.fileobj.write(str(value))
            self.fileobj.write('\n')
    else:
        def writepair(self, kvpair):
            key, value = kvpair
            self.fileobj.write(unicode(key).encode('utf-8'))
            self.fileobj.write(' ')
            self.fileobj.write(unicode(value).encode('utf-8'))
            self.fileobj.write('\n')


class HexReader(Reader):
    """A key-value store using ASCII hexadecimal encoding"""

    def __iter__(self):
        """Iterate over key-value pairs."""
        for line in self.fileobj:
            encoded_key, encoded_value = line.split()
            key, _ = hex_decoder(encoded_key)
            value, _ = hex_decoder(encoded_value)
            if self.loads_key is not None:
                key = self.loads_key(key)
            if self.loads_value is not None:
                value = self.loads_value(value)
            yield (key, value)


class HexWriter(Writer):
    """A key-value store using ASCII hexadecimal encoding

    Initialize with a file-like object.  The ASCII hexadecimal encoding of
    keys has the property that sorting the file will preserve the sort order.
    """
    ext = 'mrsx'

    def writepair(self, kvpair):
        """Write a key-value pair."""
        key, value = kvpair
        if self.dumps_key is not None:
            key = self.dumps_key(key)
        if self.dumps_value is not None:
            value = self.dumps_value(value)
        encoded_key, length = hex_encoder(key)
        encoded_value, length = hex_encoder(value)
        print(encoded_key, encoded_value, file=self.fileobj)


class BinWriter(Writer):
    """A key-value store using a simple binary record format.

    By default, the given file will be closed when the writer is closed,
    but the close argument makes this configurable.  Setting close to False
    is useful for StringIO/BytesIO.
    """
    ext = 'mrsb'
    magic = b'MrsB'

    def __init__(self, fileobj, *args, **kwds):
        super(BinWriter, self).__init__(fileobj, *args, **kwds)
        self.fileobj.write(self.magic)

    def writepair(self, kvpair):
        """Write a key-value pair."""
        key, value = kvpair
        if self.dumps_key is not None:
            key = self.dumps_key(key)
        if self.dumps_value is not None:
            value = self.dumps_value(value)

        binlen = struct.pack('<L', len(key))
        self.fileobj.write(binlen)
        self.fileobj.write(key)

        binlen = struct.pack('<L', len(value))
        self.fileobj.write(binlen)
        self.fileobj.write(value)


class BinReader(Reader):
    """A key-value store using a simple binary record format."""
    magic = b'MrsB'

    def __init__(self, fileobj, *args, **kwds):
        super(BinReader, self).__init__(fileobj, *args, **kwds)
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
            if self.loads_key is not None:
                key = self.loads_key(key)
            if self.loads_value is not None:
                value = self.loads_value(value)
            yield (key, value)

    def _fill_buffer(self, size=DEFAULT_BUFFER_SIZE):
        self._buffer += self.fileobj.read(size)

    def _read_record(self):
        LENFIELD_SIZE = 4
        if len(self._buffer) < LENFIELD_SIZE:
            self._fill_buffer()
        if not self._buffer:
            return None

        lenfield = self._buffer[:LENFIELD_SIZE]
        length, = struct.unpack('<L', lenfield)

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

    def __init__(self, fileobj, *args, **kwds):
        fileobj = gzip.GzipFile(fileobj=fileobj, mode='wb',
                compresslevel=COMPRESS_LEVEL)
        super(ZipWriter, self).__init__(fileobj, *args, **kwds)

    def finish(self):
        # Close the gzip file (which does not close the underlying file).
        self.fileobj.close()


class ZipReader(BinReader):
    """A key-value store using a simple compressed binary record format."""
    magic = b'MrsZ'

    def __init__(self, fileobj, *args, **kwds):
        self.original_file = fileobj
        fileobj = gzip.GzipFile(fileobj=fileobj, mode='rb')
        super(ZipReader, self).__init__(fileobj, *args, **kwds)

    def close(self):
        # Close the gzip file (which does not close the underlying file).
        self.fileobj.close()
        self.original_file.close()


# Note: the PickleWriter uses marginally less space than the BinWriter (and
# is simpler to read), but the Pickle Reader is some 3 times slower than the
# BinReader.  My theory that it has something to do with excessive seeks, but
# I'm not quite sure.  In any case, this format is currently inefficient.
class PickleWriter(Writer):
    """An EXPERIMENTAL key-value store using the standard pickle format.

    By default, the given file will be closed when the writer is closed,
    but the close argument makes this configurable.  Setting close to False
    is useful for StringIO/BytesIO.
    """
    ext = 'mrsp'

    def writepair(self, kvpair):
        """Write a key-value pair."""
        key, value = kvpair
        #key = self.pickler.dump(key)
        #value = self.pickler.dump(value)
        #self.pickler.clear_memo()
        pickler = pickle.Pickler(self.fileobj, -1)
        pickler.dump(key)
        pickler.dump(value)


class PickleReader(Reader):
    """An EXPERIMENTAL key-value store using the standard pickle format."""
    def __iter__(self):
        """Iterate over key-value pairs."""
        unpickler = pickle.Unpickler(self.fileobj)
        while True:
            try:
                #key = self.unpickler.load()
                key = unpickler.load()
            except EOFError:
                return
            try:
                #value = self.unpickler.load()
                value = unpickler.load()
            except EOFError:
                raise RuntimeError('File ended with a lone key')
            yield (key, value)


def writerformat(extension):
    """Returns the writer class associated with the given file extension."""
    return writer_map[extension]


def fileformat(filename):
    """Returns the Reader class associated with the given file extension."""
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_read_format)


def open_url(url, **kwds):
    """Opens a url or file and returns an appropriate key-value reader."""
    reader_cls = fileformat(url)

    parsed_url = urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path, 'rb')
    else:
        if parsed_url.scheme == 'hdfs':
            server, username, path = hdfs.urlsplit(url)
            url = hdfs.datanode_url(server, username, path)

        if reader_cls is ZipReader and sys.version_info < (3, 2):
            # In Python <3.2, the gzip module is broken because it depends on
            # the underlying file being seekable (not true for url objects).
            opener = URLopener()
            filename, _ = opener.retrieve(url)
            f = open(filename, 'rb')
            os.unlink(filename)
        else:
            f = urlopen(url)

    return reader_cls(f, **kwds)


def test():
    import doctest
    doctest.testmod()


reader_map = {
        'mrsx': HexReader,
        'mrsb': BinReader,
        'mrsp': PickleReader,
        'mrsz': ZipReader,
        }
writer_map = {
        'mtxt': TextWriter,
        'mrsx': HexWriter,
        'mrsb': BinWriter,
        'mrsp': PickleWriter,
        'mrsz': ZipWriter,
        }
default_read_format = LineReader
default_write_format = BinWriter

# vim: et sw=4 sts=4

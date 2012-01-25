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
import cStringIO
import os
import urlparse
import urllib2

from itertools import islice

hex_encoder = codecs.getencoder('hex_codec')
hex_decoder = codecs.getdecoder('hex_codec')


class LineConsumer(object):
    """Consume data (from a Producer) into a Bucket.

    In this basic consumer, the key-value pair is composed of a line number
    and line contents.  Note that the most valuable method to override in this
    class is __iter__.
    """

    def __init__(self, bucket):
        self.bucket = bucket

        self._buffer = ''

    def __iter__(self):
        """Iterate over key-value pairs.

        Inheriting classes will almost certainly override this method.
        """
        for index, line in enumerate(self.lines()):
            yield index, line

    def lines(self):
        """Iterate over complete lines in the buffer.

        Note that the lines are removed.  If the last line is a partial line
        (i.e., it doesn't have a trailing newline), it is left in the buffer.
        Also note that the buffer must be left alone while we do this.
        """
        stringio = cStringIO.StringIO(self._buffer)
        self._buffer = ''
        for line in stringio:
            if line[-1] == '\n':
                yield line
            else:
                # premature end; save partial line back to buffer
                self._buffer = line

    def write(self, data):
        self._buffer += data
        self.bucket.collect(self)


class SerialProducer(object):
    """A Producer which reads data from any URL.

    The producer will do all work when the run method is called, and it does
    not interact with the reactor in any way.  This producer should only be
    used in sequential code or in a non-reactor thread.
    """

    def __init__(self, url, consumer):
        self.url = url
        self.consumer = consumer

    def run(self):
        """Loads data and sends it to the consumer."""
        f = open_url(self.url)
        data = f.read()
        f.close()
        self.consumer.write(data)


# TODO: implement TextConsumer
# class TextConsumer(LineConsumer) should read lines and do:
# key, value = line.split(None, 1)

class TextWriter(object):
    """A basic line-oriented format, primarily for user interaction

    For writing, the key and value are separated by spaces, with one entry per
    line.
    """
    ext = 'mtxt'

    def __init__(self, file):
        self.file = file

    def writepair(self, kvpair):
        key, value = kvpair
        print >>self.file, key, value

    def close(self):
        self.file.flush()
        os.fsync(self.file.fileno())
        self.file.close()


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


def writerformat(extension):
    """Returns the writer class associated with the given file extension."""
    return writer_map[extension]


def fileformat(filename):
    """Returns the Consumer class associated with the given file extension."""
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_read_format)


def open_url(url):
    """Opens a url or file and returns a filelike object."""
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path)
    else:
        f = urllib2.urlopen(url)
    return f


def fill(url, bucket):
    """Open a url or file and write it to a bucket."""
    consumer_cls = fileformat(url)
    consumer = consumer_cls(bucket)
    producer = SerialProducer(url, consumer)
    producer.run()


def test():
    import doctest
    doctest.testmod()


reader_map = {
        'mrsx': HexConsumer,
        }
writer_map = {
        'mtxt': TextWriter,
        'mrsx': HexWriter,
        }
default_read_format = LineConsumer
default_write_format = HexWriter

# vim: et sw=4 sts=4

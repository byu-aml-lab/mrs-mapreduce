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

from consumer import LineConsumer
from textformat import TextWriter
from hexformat import HexConsumer, HexWriter, hexformat_sort

reader_map = {
        #'mtxt': TextConsumer,
        'mrsx': HexConsumer,
        }
writer_map = {
        'mtxt': TextWriter,
        'mrsx': HexWriter,
        }
default_format = LineConsumer


def writerformat(extension):
    """Returns the writer class associated with the given file extension."""
    return writer_map[extension]


def fileformat(filename):
    """Returns the Consumer class associated with the given file extension."""
    import os
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_format)


def open_url(url):
    """Opens a url or file and returns a filelike object."""
    import urlparse, urllib2
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path)
    else:
        f = urllib2.urlopen(url)
    return f


def blocking_fill(url, bucket):
    """Open a url or file and write it to a bucket."""
    from producers import SerialProducer
    consumer_cls = fileformat(url)
    consumer = consumer_cls(bucket)
    producer = SerialProducer(url, consumer)
    producer.run()


def test():
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

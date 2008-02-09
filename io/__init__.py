# Mrs
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

__all__ = ['Buffer', 'TextReader', 'TextWriter', 'HexReader', 'HexWriter',
        'hexformat_sort', 'fileformat', 'writerformat', 'openbuf']

from textformat import TextReader, TextWriter
from hexformat import HexReader, HexWriter, hexformat_sort
from buffer import Buffer

reader_map = {
        'txt': TextReader,
        'mrsx': HexReader,
        }
writer_map = {
        'txt': TextWriter,
        'mrsx': HexWriter,
        }
default_format = TextReader

def writerformat(extension):
    return writer_map[extension]

# TODO: Find a better way to infer the file format.
def fileformat(filename):
    """Guess the file format according to extension of the given filename."""
    import os
    extension = os.path.splitext(filename)[1]
    # strip the dot off:
    extension = extension[1:]
    return reader_map.get(extension, default_format)

def openbuf(url):
    """Open a url or file into a Mrs Buffer
    
    Initially, the Buffers will be empty.  However, when Twisted's
    reactor.run() is called, data will be read into all Buffers
    simultaneously.
    """
    import urlparse, urllib2
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path)
        buf = Buffer(filelike=f)
    else:
        from net import download
        buf = download(url)
    return buf

def openreader(url):
    """Open a url or file and wrap an input format around it.
    """
    buf = openbuf(url)
    format = fileformat(url)
    return format(buf)

# vim: et sw=4 sts=4

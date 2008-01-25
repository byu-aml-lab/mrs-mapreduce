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

__all__ = ['TextFormat', 'HexFormat', 'hexformat_sort', 'fileformat',
        'openfile']

from textformat import TextFormat
from hexformat import HexFormat, hexformat_sort

format_map = {
        '.txt': TextFormat,
        '.mrsx': HexFormat,
        }
default_format = TextFormat

def fileformat(filename):
    """Guess the file format according to extension of the given filename."""
    import os
    extension = os.path.splitext(filename)[1]
    return format_map.get(extension, default_format)

# TODO: Find a better way to infer the file format.
def openfile(url, mode='r'):
    """Open a url or file to be read or written as a list of key-value pairs.
    
    The file format is inferred from the filename.
    """
    import urlparse, urllib2
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path, mode)
    else:
        f = urllib2.urlopen(url)
    input_format = fileformat(url)
    return input_format(f)


# vim: et sw=4 sts=4

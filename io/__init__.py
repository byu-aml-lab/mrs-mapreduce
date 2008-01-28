#!/usr/bin/env python

__all__ = ['Buffer', 'TextFormat', 'HexFormat', 'hexformat_sort',
        'fileformat', 'openbuf']

from textformat import TextFormat
from hexformat import HexFormat, hexformat_sort
from buffer import Buffer

format_map = {
        '.txt': TextFormat,
        '.mrsx': HexFormat,
        }
default_format = TextFormat

# TODO: Find a better way to infer the file format.
def fileformat(filename):
    """Guess the file format according to extension of the given filename."""
    import os
    extension = os.path.splitext(filename)[1]
    return format_map.get(extension, default_format)

def openbuf(url, mode='r'):
    """Open a url or file into a Mrs Buffer
    
    Initially, the Buffers will be empty.  However, when Twisted's
    reactor.run() is called, data will be read into all Buffers
    simultaneously.
    """
    import urlparse, urllib2
    parsed_url = urlparse.urlparse(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path, mode)
        buf = Buffer(filelike=f)
    else:
        from net import download
        buf = download(url)
    return buf


# vim: et sw=4 sts=4

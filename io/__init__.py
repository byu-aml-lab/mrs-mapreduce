#!/usr/bin/env python

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

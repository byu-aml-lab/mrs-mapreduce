#!/usr/bin/env python

from textfile import TextFile
from hexfile import HexFile, hexfile_sort

format_map = {
        '.txt': TextFile,
        '.hexfile': HexFile,
        }
default_format = TextFile

def fileformat(filename):
    import os
    extension = os.path.splitext(filename)[1]
    return format_map.get(extension, default_format)

__all__ = ['TextFile', 'HexFile', 'hexfile_sort', 'fileformat']

# vim: et sw=4 sts=4

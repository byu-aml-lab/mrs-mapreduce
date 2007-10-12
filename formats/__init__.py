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

from textfile import TextFile
from hexfile import HexFile, hexfile_sort

format_map = {
        '.txt': TextFile,
        '.hexfile': HexFile,
        }
default_format = TextFile

def fileformat(filename):
    import os
    extension = os.path.split_ext(filename)[1]
    return format_map.get(extension, default_format)

__all__ = ['TextFile', 'HexFile', 'hexfile_sort', 'fileformat']

# vim: et sw=4 sts=4

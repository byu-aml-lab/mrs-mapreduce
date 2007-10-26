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

from itertools import islice


class TextFile(object):
    """A file format for user interaction.

    Initialize with a file object.  For reading, the key is the line number,
    and the value is the contents of the line.  For writing, the key and value
    are separated by spaces, with one entry per line.
    """
    def __init__(self, textfile):
        self.file = textfile
        self.linenumber = 0

    def __iter__(self):
        return self

    def read(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        self.linenumber += 1
        line = self.file.readline()
        if line:
            return (self.linenumber, line)
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        self.linenumber += 1
        line = self.file.next()
        return (self.linenumber, line)

    def write(self, key, value):
        print >>self.file, key, value

    def close(self):
        self.file.close()


# vim: et sw=4 sts=4

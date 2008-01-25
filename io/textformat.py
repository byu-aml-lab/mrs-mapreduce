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


class TextFormat(object):
    """A basic line-oriented file format, primarily for user interaction

    Initialize with a file object.  For reading, the key is the file offset,
    and the value is the contents of the line.  For writing, the key and value
    are separated by spaces, with one entry per line.

    >>> from cStringIO import StringIO
    >>> infile = StringIO("First Line.\\nSecond Line.\\nText without newline.")
    >>> text = TextFormat(infile)
    >>> text.readpair()
    (0, 'First Line.\\n')
    >>> text.readpair()
    (12, 'Second Line.\\n')
    >>> text.readpair()
    >>>
    """
    def __init__(self, textfile):
        self.file = textfile
        self.offset = 0
        self._buffer = ''

    def __iter__(self):
        return self

    def readline(self):
        """Try to read in a line from the underlying input file.

        If no full line is available, then seek back and return ''.
        """
        self.offset = self.file.tell()
        line = self.file.readline()
        if line is '':
            return ''
        elif line[-1] != '\n':
            self.file.seek(self.offset)
            return ''
        else:
            return line

    def readpair(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        line = self.readline()
        if line is '':
            return None
        else:
            return (self.offset, line)

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.readline()
        if line is '':
            raise StopIteration
        else:
            return (self.offset, line)

    def write(self, key, value):
        print >>self.file, key, value

    def close(self):
        self.file.close()


def test_textformat():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test_textformat()

# vim: et sw=4 sts=4

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

from itertools import islice


class TextReader(object):
    """A basic line-oriented format, primarily for user interaction

    Initialize with a Mrs Buffer.  The key is the line number, and the value
    is the contents of the line.
    
    Create a file-like object to play around with:
    >>> from cStringIO import StringIO
    >>> infile = StringIO("First Line.\\nSecond Line.\\nText without newline.")
    >>>

    Create a buffer from this file-like object and read into the buffer:
    >>> from buffer import Buffer
    >>> buf = Buffer(infile)
    >>> buf.doRead()
    >>>

    Create a TextReader to read from the Buffer
    >>> text = TextReader(buf)
    >>> text.readpair()
    (0, 'First Line.\\n')
    >>> text.readpair()
    (1, 'Second Line.\\n')
    >>> text.readpair()
    >>>
    """

    def __init__(self, buf):
        self.buf = buf
        self.lineno = 0

    def __iter__(self):
        return self

    def readpair(self):
        """Return the next key-value pair from the HexFile or None if EOF."""
        line = self.buf.readline()
        if line is not None:
            value = (self.lineno, line)
            self.lineno += 1
            return value
        else:
            return None

    def next(self):
        """Return the next key-value pair or raise StopIteration if EOF."""
        line = self.buf.readline()
        if line is not None:
            value = (self.lineno, line)
            self.lineno += 1
            return value
        else:
            raise StopIteration


class TextWriter(object):
    """A basic line-oriented format, primarily for user interaction

    For writing, the key and value are separated by spaces, with one entry per
    line.
    """
    ext = 'txt'

    def __init__(self, file):
        self.file = file

    def writepair(self, key, value):
        print >>self.file, key, value

    def close(self):
        self.file.close()


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

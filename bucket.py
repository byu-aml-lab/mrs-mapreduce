# Mrs
# Copyright 2008-2009 Brigham Young University
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


from logging import getLogger
logger = getLogger('mrs')


# TODO: cache data to disk when memory usage is high
class Bucket(object):
    """Hold data from a source or for a split.

    Data can be manually dumped to disk, in which case the data will be saved
    to the given filename with the specified format.

    Attributes:
        source: An integer showing which source the data come from.
        split: An integer showing which split the data is directed to.
        dir: A string specifying the directory for writes.
        format: The class to be used for formatting writes.
        url: A string showing a URL that can be used to read the data.

    >>> b = Bucket(0, 0)
    >>> b.addpair((4, 'test'))
    >>> b.collect([(3, 'a'), (1, 'This'), (2, 'is')])
    >>> ' '.join(value for key, value in b)
    'test a This is'
    >>> b.sort()
    >>> ' '.join(value for key, value in b)
    'This is a test'
    >>>
    """
    def __init__(self, source, split, dir=None, format=None):
        self._data = []
        self.source = source
        self.split = split
        self.dir = dir
        if format is None:
            from io.hexformat import HexWriter
            format = HexWriter
        self.format = format

        self.url = None
        self._writer = None

    def open_writer(self):
        if self.dir and not self._writer:
            import os
            # Note that Python 2.6 has NamedTemporaryFile(delete=False), which
            # would make this easier.
            import tempfile
            fd, filename = tempfile.mkstemp(dir=self.dir, prefix=self.prefix(),
                    suffix='.' + self.format.ext)
            output_file = os.fdopen(fd, 'a')
            self._writer = self.format(output_file)

            # For now, the externally visible url is just the filename on the
            # local or networked filesystem.
            self.url = filename

    def close_writer(self):
        if self._writer:
            self._writer.close()
            self._writer = None

    def addpair(self, kvpair):
        """Collect a single key-value pair."""
        self._data.append(kvpair)
        if self.dir:
            if not self._writer:
                self.open_writer()
            self._writer.writepair(kvpair)

    def collect(self, pairiter):
        """Collect all key-value pairs from the given iterable

        The collection can be a generator or a Mrs format.  This will block if
        the iterator blocks.
        """
        data = self._data
        if self.dir:
            if not self._writer:
                self.open_writer()
            for kvpair in pairiter:
                data.append(kvpair)
                self._writer.writepair(kvpair)
        else:
            for kvpair in pairiter:
                data.append(kvpair)

    def prefix(self):
        """Return the filename for the output split for the given index.
        
        >>> b = Bucket(2, 4)
        >>> b.prefix()
        'source_2_split_4_'
        >>>
        """
        return 'source_%s_split_%s_' % (self.source, self.split)

    def sort(self):
        self._data.sort()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        """Get a particular item, mainly for debugging purposes"""
        return self._data[item]

    def __iter__(self):
        return iter(self._data)


# vim: et sw=4 sts=4

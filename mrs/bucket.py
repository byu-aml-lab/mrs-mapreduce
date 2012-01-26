# Mrs
# Copyright 2008-2011 Brigham Young University
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

import cStringIO
import os
import urlparse

from . import fileformats

from logging import getLogger
logger = getLogger('mrs')


class ReadBucket(object):
    """Hold data from a source.

    Attributes:
        source: An integer showing which source the data come from.
        split: An integer showing which split the data is directed to.
        url: A string showing a URL that can be used to read the data.
    """
    def __init__(self, source, split):
        self._data = []
        self.source = source
        self.split = split
        self.url = None

    def addpair(self, kvpair):
        """Collect a single key-value pair."""
        self._data.append(kvpair)

    def collect(self, pairiter):
        """Collect all key-value pairs from the given iterable

        The collection can be a generator or a Mrs format.  This will block if
        the iterator blocks.
        """
        data = self._data
        for kvpair in pairiter:
            data.append(kvpair)

    def sort(self):
        self._data.sort()

    def clean(self):
        """Removes any temporary files created and empties cached data."""
        self._data = None

    def __getstate__(self):
        """Pickle (serialize) the bucket."""
        state = self.__dict__.copy()
        buf = cStringIO.StringIO()
        with fileformats.BinWriter(buf) as writer:
            for pair in self._data:
                writer.writepair(pair)
        state['_data'] = buf.getvalue()
        buf.close()
        return state

    def __setstate__(self, state):
        """Unpickle (deserialize) the bucket."""
        self.__dict__ = state
        # TODO: Python 3 supports using context managers with BytesIO
        #with io.BytesIO(self._data) as buf:
        buf = cStringIO.StringIO(self._data)
        self._data = []
        reader = fileformats.BinReader(buf)
        self.collect(reader)
        buf.close()

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        """Get a particular item, mainly for debugging purposes"""
        return self._data[item]

    def __iter__(self):
        return iter(self._data)


class WriteBucket(ReadBucket):
    """Hold data for a split.

    Data can be manually dumped to disk, in which case the data will be saved
    to the given filename with the specified format.

    Attributes:
        source: An integer showing which source the data come from.
        split: An integer showing which split the data is directed to.
        dir: A string specifying the directory for writes.
        format: The class to be used for formatting writes.
        path: The local path of the written file.

    >>> b = WriteBucket(0, 0)
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
        super(WriteBucket, self).__init__(source, split)
        self.dir = dir
        if format is None:
            format = fileformats.default_write_format
        self.format = format

        self._output_file = None
        self._writer = None

    def __setstate__(self, state):
        raise NotImplementedError

    def readonly_copy(self):
        b = ReadBucket(self.source, self.split)
        b._data = self._data
        b.url = self.url
        return b

    def open_writer(self):
        # Don't open if 1) there's no place to put it; 2) it's already saved
        # some place; or 3) it's already open.
        if self.dir and not self.url and not self._writer:
            # Note that Python 2.6 has NamedTemporaryFile(delete=False), which
            # would make this easier.
            import tempfile
            fd, self.url = tempfile.mkstemp(dir=self.dir,
                    prefix=self.prefix(), suffix='.' + self.format.ext)
            self._output_file = os.fdopen(fd, 'a')
            self._writer = self.format(self._output_file)

    def close_writer(self):
        if self._writer:
            self._writer.finish()
            self._writer = None
        if self._output_file:
            self._output_file.flush()
            os.fsync(self._output_file.fileno())
            self._output_file.close()
            self._output_file = None

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

    def clean(self):
        """Removes any temporary files created and empties cached data."""
        super(WriteBucket, self).clean()
        self._data = None
        if self.url:
            os.remove(parsed_url.path)


class URLConverter(object):
    def __init__(self, addr, port, basedir):
        assert port is not None
        self.addr = addr
        self.port = port
        self.netloc = '%s:%s' % (addr, port)
        self.basedir = basedir

    def local_to_global(self, path):
        """Creates a URL corresponding to the given path."""
        url_path = os.path.relpath(path, self.basedir)
        if url_path.startswith('..'):
            # Can't create a global URL because the file isn't in the HTTP dir.
            return path
        else:
            url_components = ('http', self.netloc, url_path, None, None, None)
            url = urlparse.urlunparse(url_components)
            return url

    def global_to_local(self, url, master):
        """Creates a locally accessible URL from the given URL.

        The urls, as sent by the master, may have any empty host fields in the
        urls if any data was created on the master.  Also, it may be possible
        to create a locally-accessible path in place of an http url.
        """
        result = urlparse.urlparse(url)
        if result.scheme and not result.hostname:
            components = list(result)
            components[1] = master
            if result.port:
                components[1] += ':%s' % result.port
            url = urlparse.urlunparse(components)
        elif (result.hostname == self.addr) and (result.port == self.port):
            path = result.path.lstrip('/')
            url = os.path.join(self.basedir, path)
        return url


# vim: et sw=4 sts=4

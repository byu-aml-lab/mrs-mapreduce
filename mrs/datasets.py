# Mrs
# Copyright 2008-2012 Brigham Young University
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


# TODO: add a Dataset for resplitting input (right now we assume that input
# files are pre-split).

import collections
from itertools import chain

from . import bucket
from . import fileformats
from . import util

from logging import getLogger
logger = getLogger('mrs')

DATASET_ID_LENGTH = 8


class BaseDataset(object):
    """Manage input to or output from a map or reduce operation.

    A Dataset is naturally a two-dimensional list of buckets.  There are some
    number of sources, and for each source, there are one or more splits.

    Datasets supports access via the subscript operator.  In addition to
    retrieving/setting individual buckets, the subscript operator supports
    slicing for retrieving a view of buckets.  For example, `ds[1, :]` would
    give an iterable view containing the buckets from source 1 for any split.

    Attributes:
        splits: number of outputs per source; to get all data for a particular
            split from all sources, use splitdata()
        serializers: a Serializers instance that keeps track of serializers
            and their associated names.
    """
    def __init__(self, splits=0, dir=None, format=None, permanent=True,
            serializers=None):
        self.splits = splits
        self.dir = dir
        self.format = format
        self.permanent = permanent
        self.serializers = serializers

        self.id = util.random_string(DATASET_ID_LENGTH)
        self.closed = False
        self._close_callback = None
        self._extended_sources = 0

        self._data = {}
        self._splits_per_source = collections.defaultdict(set)
        self._sources_per_split = collections.defaultdict(set)

    def _make_bucket(self, source, split):
        """Overridable method for creating a new bucket."""
        raise NotImplementedError

    def extend_split(self, split, buckets):
        """Extend a split from an iterable collection of buckets.

        Each bucket will be assigned a arbitrary negative source number.
        """
        for bucket in buckets:
            self._extended_sources += 1
            source = -self._extended_sources
            self[source, split] = bucket

    def __bool__(self):
        return True

    # For Python 2:
    def __nonzero__(self):
        return True

    def data(self):
        """Iterate over data from all buckets."""
        buckets = self[:, :]
        return chain.from_iterable(buckets)

    def splitdata(self, split):
        """Iterate over data from buckets for a given split."""
        buckets = self[:, split]
        return chain.from_iterable(buckets)

    def sourcedata(self, source):
        """Iterate over data from buckets for a given source."""
        buckets = self[source, :]
        return chain.from_iterable(buckets)

    def _set_close_callback(self, callback):
        self._close_callback = callback

    def close(self):
        """Close Dataset for future use.

        No additional Datasets will be able to depend on this Dataset for
        input, and no further reads will be allowed.  Calling close() allows
        the system to free resources.  Don't close a Dataset unless you really
        mean it.
        """
        assert not self.closed
        self.closed = True
        if self._close_callback:
            self._close_callback(self)

    def clear(self):
        self._data = None

    def delete(self):
        """Delete current data and temporary files from the dataset."""
        for b in self[:, :]:
            b.clean()
        if self.dir:
            # Just to make sure it's all gone:
            util.remove_recursive(self.dir)
        self.clear()

    def ready(self):
        """Report whether Dataset is ready.

        Ready means that the input Dataset is done(), so this Dataset can be
        computed without waiting.  For most types of Datasets, this is
        automatically true.
        """
        return True

    def fetchall(self, *args, **kwds):
        """Download all of the files.

        For most types of Datasets, this is a no-op.
        """
        return

    def __setitem__(self, key, bucket):
        self._data[key] = bucket
        source, split = key
        self._splits_per_source[source].add(split)
        self._sources_per_split[split].add(source)

    def __getitem__(self, key_pair):
        """Retrieve a bucket or iterator, given a (source_key, split_key) pair.

        Returns an iterator over buckets if either key is a slice.
        """
        # Separate the two dimensions:
        try:
            source_key, split_key = key_pair
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of keys.")

        source_key_is_slice = isinstance(source_key, slice)
        split_key_is_slice = isinstance(split_key, slice)

        if not source_key_is_slice and not split_key_is_slice:
            # Special case: single bucket
            try:
                bucket = self._data[key_pair]
            except KeyError:
                bucket = self._make_bucket(source_key, split_key)
                self[key_pair] = bucket
            return bucket

        if ((source_key_is_slice and (source_key.start != None or
                source_key.stop != None or  source_key.step != None)) or
                (split_key_is_slice and (split_key.start != None or
                split_key.stop != None or  split_key.step != None))):
            raise TypeError("General slicing is not supported")

        data = self._data

        if source_key_is_slice and split_key_is_slice:
            return data.values()
        elif split_key_is_slice:
            splits = self._splits_per_source.get(source_key)
            if splits is None:
                return iter(())
            else:
                return (data[source_key, y] for y in splits)
        elif source_key_is_slice:
            sources = self._sources_per_split.get(split_key)
            if sources is None:
                return iter(())
            else:
                return (data[x, split_key] for x in sources)

    # The __iter__ method must be defined because the default iterator falls
    # back on __getitem__ and goes horribly wrong.
    def __iter__(self):
        raise TypeError('Dataset object is not iterable')

    def __del__(self):
        if not self.closed:
            self.close()


class LocalData(BaseDataset):
    """Collect output from an iterator.

    If the `parter` function is specified, then it is used to partition data
    by keys.  The `splits` parameter defines the total number of buckets.  If
    the `parter` is unspecified, then data are assigned to buckets on a
    round-robin basis, and if `splits` is also unspecified, then the number of
    buckets will grow with the size of the iterator.

    Note that the `source`, which is just used for naming files, represents
    which output source is being created.

    >>> lst = [(4, 'to_0'), (5, 'to_1'), (7, 'to_3'), (9, 'to_1')]
    >>> o = LocalData(lst, splits=4, parter=(lambda x, n: x%n))
    >>> list(o[0, 1])
    [(5, 'to_1'), (9, 'to_1')]
    >>> list(o[0, 3])
    [(7, 'to_3')]
    >>>
    """
    def __init__(self, itr, splits=None, source=0, parter=None,
            **kwds):
        if parter is not None and splits is None:
            raise RuntimeError('The splits parameter is required when parter'
                    ' is specified.')

        super(LocalData, self).__init__(splits=splits, **kwds)
        self.id = 'local_' + self.id
        self.fixed_source = source

        self.collected = False
        self._collect(itr, parter)
        for key, bucket in self._data.items():
            self._data[key] = bucket.readonly_copy()
        self.collected = True

    def _make_bucket(self, source, split):
        assert not self.collected
        assert source == self.fixed_source
        return bucket.WriteBucket(source, split, self.dir, self.format,
                serializers=self.serializers)

    def _collect(self, itr, parter):
        """Collect all of the key-value pairs from the given iterator."""
        n = self.splits
        source = self.fixed_source
        if parter is None:
            if n:
                # Assign to buckets in a round-robin fashion.
                for split, kvpair in enumerate(itr):
                    bucket = self[source, split % n]
                    bucket.addpair(kvpair)
            else:
                # Grow the number of buckets with the size of the data.
                for split, kvpair in enumerate(itr):
                    bucket = self[source, split]
                    bucket.addpair(kvpair)
                self.splits = split + 1
        else:
            if n == 1:
                bucket = self[source, 0]
                bucket.collect(itr)
            else:
                for kvpair in itr:
                    key, value = kvpair
                    split = parter(key, n)
                    bucket = self[source, split]
                    bucket.addpair(kvpair)
        for bucket in self[:, :]:
            bucket.close_writer(self.permanent)


class RemoteData(BaseDataset):
    """A Dataset whose contents can be downloaded and read.

    Subclasses need to set the url for each bucket.
    """
    def __init__(self, **kwds):
        super(RemoteData, self).__init__(**kwds)

        self._urls_known = False
        self._fetched = False

    def _make_bucket(self, source, split):
        return bucket.ReadBucket(source, split, serializers=self.serializers)

    def __getstate__(self):
        """Pickle without getting certain forbidden/unnecessary elements."""
        state = self.__dict__.copy()
        del state['_close_callback']
        del state['_fetched']
        if self.closed:
            state['_data'] = None
        return state

    def __setstate__(self, dict):
        self.__dict__ = dict
        self._close_callback = None
        self._fetched = False

    # TODO: consider parallelizing this to use multiple downloading threads.
    def fetchall(self):
        """Download all of the files."""
        assert not self.closed, (
                'Invalid fetchall on a closed dataset.')

        # Don't call fetchall twice:
        if self._fetched:
            return

        assert self._urls_known, (
                'Invalid fetchall on a dataset with unknown urls.')

        kwds = {}
        if self.serializers:
            kwds['serializers'] = self.serializers

        for bucket in self[:, :]:
            url = bucket.url
            if url:
                with fileformats.open_url(url, **kwds) as reader:
                    bucket.collect(reader)

        self._fetched = True

    def notify_urls_known(self):
        """Signify that all buckets have been assigned urls."""
        self._urls_known = True


class FileData(RemoteData):
    """A list of static files or urls to be used as input to an operation.

    By default, all of the files come from a single source, with one split for
    each file.  If a split is given, then the dataset will have enough sources
    to evenly divide the files.

    >>> urls = ['http://aml.cs.byu.edu/', 'LICENSE']
    >>> data = FileData(urls)
    >>> len(data[:, :])
    2
    >>> data.fetchall()
    >>> data[0, 0][0] == (0, b'<html>\\n')
    True
    >>> data[0, 0][1] == (1, b'<head>\\n')
    True
    >>> key, value = data[0, 1][0]
    >>> value.strip() == b'GNU GENERAL PUBLIC LICENSE'
    True
    >>>
    """
    def __init__(self, urls, sources=None, splits=None,
            first_source=0, first_split=0, **kwds):
        n = len(urls)

        if splits is None:
            if sources is None:
                # Nothing specified, so we assume one split per url
                splits = n
            else:
                splits = first_split + n // sources

        super(FileData, self).__init__(splits=splits, **kwds)
        for i, url in enumerate(urls):
            if url:
                source = first_source + i // splits
                split = first_split + i % splits
                bucket = self[source, split]
                bucket.url = url
        self._urls_known = True


def test():
    import doctest
    doctest.testmod()


# vim: et sw=4 sts=4

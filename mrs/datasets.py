# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# TODO: add a Dataset for resplitting input (right now we assume that input
# files are pre-split).

import collections
import heapq
from itertools import chain
from operator import itemgetter
import random
import tempfile

from . import bucket
from . import fileformats
from .serializers import (dumps_functions, loads_functions, raw_serializer,
        Serializers)
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

    def _assert_open(self, _called_in_runner):
        """Assert that the dataset is not closed.

        If called from within the runner thread (as determined by the given
        parameter), then operations on closed datasets are permitted.
        """
        assert _called_in_runner or not self.closed, (
                'Invalid operation on a closed dataset.')

    def data(self):
        """Iterate over data from all buckets."""
        buckets = self[:, :]
        return chain.from_iterable(buckets)

    def stream_data(self, _called_in_runner=False):
        """Iterate over all key-value pairs in the dataset."""
        self._assert_open(_called_in_runner)
        return self.data()

    def splitdata(self, split, _called_in_runner=False):
        """Iterate over data from buckets for a given split."""
        self._assert_open(_called_in_runner)
        buckets = self[:, split]
        return chain.from_iterable(buckets)

    def stream_split(self, split):
        """Iterate over data from buckets for a given split."""
        return self.splitdata(split)

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

    def __getstate__(self):
        """Pickle without getting certain forbidden/unnecessary elements."""
        state = self.__dict__.copy()
        serializers = self.serializers
        if serializers is not None:
            state['serializers'] = (serializers.key_s_name,
                    serializers.value_s_name)
        return state


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
            write_only=False, **kwds):
        if parter is not None and splits is None:
            raise RuntimeError('The splits parameter is required when parter'
                    ' is specified.')

        super(LocalData, self).__init__(splits=splits, **kwds)
        self.id = 'local_' + self.id
        self.fixed_source = source

        self.collected = False
        self._collect(itr, parter, write_only)
        for key, bucket in self._data.items():
            self._data[key] = bucket.readonly_copy()
        self.collected = True

    def _make_bucket(self, source, split):
        assert not self.collected
        assert source == self.fixed_source
        return bucket.WriteBucket(source, split, self.dir, self.format,
                serializers=self.serializers)

    def _collect(self, itr, parter, write_only):
        """Collect all of the key-value pairs from the given iterator."""
        n = self.splits
        source = self.fixed_source
        if parter is None:
            if n:
                # Assign to buckets in a round-robin fashion.
                for split, kvpair in enumerate(itr):
                    bucket = self[source, split % n]
                    bucket.addpair(kvpair, write_only)
            else:
                # Grow the number of buckets with the size of the data.
                for split, kvpair in enumerate(itr):
                    bucket = self[source, split]
                    bucket.addpair(kvpair, write_only)
                self.splits = split + 1
        else:
            if n == 1:
                bucket = self[source, 0]
                bucket.collect(itr, write_only)
            else:
                for kvpair in itr:
                    key, value = kvpair
                    split = parter(key, n)
                    bucket = self[source, split]
                    bucket.addpair(kvpair, write_only)
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
        state = super(RemoteData, self).__getstate__()
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
    def fetchall(self, _called_in_runner=False):
        """Download all of the files."""

        # Don't call fetchall twice.
        if self._fetched:
            return

        self._assert_open(_called_in_runner)
        assert self._urls_known, (
                'Invalid fetchall on a dataset with unknown urls.')

        kwds = {}
        if self.serializers:
            kwds['serializers'] = self.serializers

        # Collect the buckets in shuffled order to reduce the cost of many
        # tasks hitting the same machines at the same time.
        buckets = [bucket for bucket in self[:, :] if bucket.url]
        random.shuffle(buckets)
        for bucket in buckets:
            with fileformats.open_url(bucket.url, **kwds) as reader:
                bucket.collect(reader)

        self._fetched = True

    def _stream_buckets(self, buckets, serializers):
        streams = (b.stream(serializers) for b in buckets)
        return chain.from_iterable(streams)

    def stream_data(self, serializers=None, _called_in_runner=False):
        """Iterate over all remote key-value pairs in the dataset."""
        self._assert_open(_called_in_runner)
        if self._fetched:
            return self.data()

        buckets = [bucket for bucket in self[:, :] if bucket.url]
        random.shuffle(buckets)
        return self._stream_buckets(buckets, serializers)

    def stream_split(self, split, serializers=None, _called_in_runner=False):
        self._assert_open(_called_in_runner)
        if self._fetched:
            return self.splitdata(split)

        buckets = [bucket for bucket in self[:, split] if bucket.url]
        random.shuffle(buckets)
        return self._stream_buckets(buckets, serializers)

    def notify_urls_known(self):
        """Signify that all buckets have been assigned urls."""
        self._urls_known = True


class MergeSortData(BaseDataset):
    """A locally stored copy, sorted by key, of another dataset.

    If the dataset is small enough, it will be stored in RAM.  Otherwise,
    it will be stored in local temporary files.

    Note that this class is very specific in its purpose and applicability.
    """
    def __init__(self, input, input_split, max_sort_size, splits=None,
            source=None, parter=None, _called_in_runner=False, **kwds):
        if parter is not None:
            raise RuntimeError('The parter paramater must not be specified')
        if source is not None:
            raise RuntimeError('The source paramater must not be specified')
        if splits is not None:
            raise RuntimeError('The splits paramater must not be specified')

        super(MergeSortData, self).__init__(splits=splits, **kwds)
        self.id = 'mergesort_' + self.id
        self.fixed_split = input_split
        self.serializers = input.serializers
        self.permanent = False

        self.collected = False
        self._collect(input, input_split, max_sort_size, _called_in_runner)
        self.collected = True

    def _collect(self, input, input_split, max_sort_size, _called_in_runner):
        assert not self.collected
        loads_key, loads_value = loads_functions(input.serializers)
        raw_serializers = Serializers(raw_serializer, 'raw_serializer',
                raw_serializer, 'raw_serializer')
        max_ram_bytes = 1024 * 1024 * max_sort_size

        current_bytes = 0
        total_bytes = 0
        data_list = []
        for raw_key, raw_value in input.stream_split(input_split,
                serializers=raw_serializers,
                _called_in_runner=_called_in_runner):
            pair_bytes = len(raw_key) + len(raw_value)
            if current_bytes + pair_bytes > max_ram_bytes:
                data_list.sort(key=itemgetter(0))
                self._flush_data(data_list, raw_serializers, input.serializers)
                data_list = []
                current_bytes = 0

            if loads_key is None:
                data_list.append((raw_key, raw_value))
            else:
                key = loads_key(raw_key)
                data_list.append((key, raw_key, raw_value))
            current_bytes += pair_bytes
            total_bytes += pair_bytes

        if self._data:
            data_list.sort(key=itemgetter(0))
            self._flush_data(data_list, raw_serializers, input.serializers)
        else:
            data_list.sort(key=itemgetter(0))
            b = bucket.WriteBucket(0, self.fixed_split)
            data_itr = self._iter_deserialized(data_list, loads_key,
                    loads_value)
            b.collect(data_itr)
            self._append_bucket(b)

        logger.debug('MergeSortData initialized %s bytes in %s buckets'
                % (total_bytes, len(self._data)))

    def _iter_deserialized(self, data_list, loads_key, loads_value):
        """Iterate over the deserialized key-value pairs of the data list."""
        if loads_key is None and loads_value is None:
            return data_list
        elif loads_key is None:
            return ((k, loads_value(raw_v)) for (k, raw_v) in data_list)
        elif loads_value is None:
            return ((k, raw_v) for (k, _, raw_v) in data_list)
        else:
            return ((k, loads_value(raw_v)) for (k, _, raw_v) in data_list)

    def _iter_serialized(self, data_list, loads_key):
        """Iterate over the deserialized key-value pairs of the data list."""
        if loads_key is None:
            return data_list
        else:
            return ((raw_k, raw_v) for (k, raw_k, raw_v) in data_list)

    def _flush_data(self, data_list, serializers, input_serializers):
        if not data_list:
            return
        b = bucket.WriteBucket(len(self._data), self.fixed_split,
                self.dir, serializers=serializers)
        loads_key, _ = loads_functions(input_serializers)
        data_itr = self._iter_serialized(data_list, loads_key)
        b.collect(data_itr, write_only=True)
        b.serializers = input_serializers
        b.close_writer(False)
        self._append_bucket(b)
        del data_list[:]

    def _append_bucket(self, b):
        b = b.readonly_copy()
        self._data[b.source, b.split] = b

    def stream_data(self, serializers=None, _called_in_runner=False):
        """Iterate over data from all buckets in key-sorted order."""
        streams = [b.stream(serializers) for b in self[:, :]]
        return heapq.merge(*streams)


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

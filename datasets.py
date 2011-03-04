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


# TODO: add a Dataset for resplitting input (right now we assume that input
# files are pre-split).

import os
import threading

from itertools import chain, izip

from . import bucket
from . import util
from .io import load

from logging import getLogger
logger = getLogger('mrs')

DATASET_ID_LENGTH = 10


class BaseDataset(object):
    """Manage input to or output from a map or reduce operation.

    A Dataset is naturally a two-dimensional list.  There are some number of
    sources, and for each source, there are one or more splits.

    Attributes:
        sources: number of input sources (e.g., tasks); to get all of the
            data from a particular source, use itersource()
        splits: number of outputs per source; to get all data for a particular
            split from all sources, use itersplit()
    """
    def __init__(self, sources=0, splits=0, dir=None, format=None,
            permanent=True, **kwds):
        self.id = util.random_string(DATASET_ID_LENGTH)
        self.sources = sources
        self.splits = splits
        self.dir = dir
        self.format = format
        self.permanent = permanent
        self.closed = False
        self._data = None

    def __len__(self):
        """Number of buckets in this Dataset."""
        raise NotImplementedError

    def __iter__(self):
        """Iterate over all buckets."""
        raise NotImplementedError

    def iterdata(self):
        """Iterate over data from all buckets."""
        return chain(*self)

    def itersplit(self, split):
        """Iterate over data from buckets for a given split."""
        buckets = self[:, split]
        return chain(*buckets)

    def itersource(self, source):
        """Iterate over data from buckets for a given source."""
        buckets = self[source, :]
        return chain(*buckets)

    def _set_close_callback(self, callback):
        self._close_callback = callback

    def close(self):
        """Close Dataset for future use.

        No additional Datasets will be able to depend on this Dataset for
        input, and no further reads will be allowed.  Calling close() allows
        the system to free resources.  Don't close a Dataset unless you really
        mean it.
        """
        self.closed = True

    def _delete(self):
        """Delete current data and temporary files from the dataset."""
        # TODO: deletion of remote datasets needs to be delegated to the
        # slave that created them.
        if self._data is None:
            return
        if not self.permanent:
            for b in self:
                b.clean()
            if self.dir:
                # Just to make sure it's all gone:
                util.remove_recursive(self.dir)
        self._data = None

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

    def __setitem__(self, item, value):
        """Set an item.

        For now, you can only set a split in the second dimension.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        if isinstance(part1, slice):
            raise NotImplementedError

        self._data[part1][part2] = value

    def __getitem__(self, item):
        """Retrieve an item or split.

        At the moment, we're not very consistent about whether what we return
        is a view or a [shallow] copy.  Write at your own risk.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        data = self._data
        if isinstance(part1, slice):
            lst = []
            wild_goose_chase = True
            for sourcelst in data[part1]:
                try:
                    lst.append(sourcelst[part2])
                    wild_goose_chase = False
                except IndexError:
                    lst.append([])

            if wild_goose_chase:
                # every sourcelst[part2] raised an indexerror
                raise IndexError("No items matching %s" % part2)

            return lst

        else:
            return self._data[part1][part2]

    def __del__(self):
        if self._data:
            self._delete()


class Dataset(BaseDataset):
    """Manage input to or output from a map or reduce operation.

    A Dataset is naturally a two-dimensional list.  There are some number of
    sources, and for each source, there are one or more splits.

    Low-level Testing.  Normally a Dataset holds Buckets, but for now we'll be
    loose for testing purposes.  This also makes it clear how slicing works,
    so it's not a waste of space.
    >>> ds = Dataset(sources=3, splits=3)
    >>> len(ds)
    9
    >>> ds[0, 0] = 'zero'
    >>> ds[0, 1] = 'one'
    >>> ds[0, 2] = 'two'
    >>> ds[1, 0] = None
    >>> ds[1, 1] = 'hello'
    >>> ds[1, 2] = None
    >>> ds[2, 1] = None
    >>> print ds[0, 1]
    one
    >>> print ds[1, 0]
    None
    >>> print ds[0, 1:]
    ['one', 'two']
    >>> print ds[0, :]
    ['zero', 'one', 'two']
    >>> print ds[0:2, 1:3]
    [['one', 'two'], ['hello', None]]
    >>> print ds[:, 1]
    ['one', 'hello', None]
    >>>
    """
    def __init__(self, **kwds):
        super(Dataset, self).__init__(**kwds)

        # For now assume that all sources have the same # of splits.
        self._data = [[bucket.Bucket(i, j)
                for j in xrange(self.splits)]
                for i in xrange(self.sources)]

    def __len__(self):
        """Number of buckets in this Dataset."""
        return sum(len(source) for source in self._data)

    def __iter__(self):
        """Iterate over all buckets."""
        return chain(*self._data)

    def __setitem__(self, item, value):
        """Set an item.

        For now, you can only set a split in the second dimension.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        if isinstance(part1, slice):
            raise NotImplementedError

        self._data[part1][part2] = value

    def __getitem__(self, item):
        """Retrieve an item or split.

        At the moment, we're not very consistent about whether what we return
        is a view or a [shallow] copy.  Write at your own risk.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        data = self._data
        if isinstance(part1, slice):
            lst = []
            wild_goose_chase = True
            for sourcelst in data[part1]:
                try:
                    lst.append(sourcelst[part2])
                    wild_goose_chase = False
                except IndexError:
                    lst.append([])

            if wild_goose_chase:
                # every sourcelst[part2] raised an indexerror
                raise IndexError("No items matching %s" % part2)

            return lst

        else:
            return self._data[part1][part2]


class LocalData(BaseDataset):
    """Collect output from a map or reduce task.

    This is only used on the slave side.  It takes a partition function and a
    number of splits to use.  Note that the `source`, which is just used for
    naming files, represents which output source is being created.

    >>> lst = [(4, 'to_0'), (5, 'to_1'), (7, 'to_3'), (9, 'to_1')]
    >>> o = LocalData(lst, splits=4, parter=(lambda x, n: x%n))
    >>> list(o[0, 1])
    [(5, 'to_1'), (9, 'to_1')]
    >>> list(o[0, 3])
    [(7, 'to_3')]
    >>>
    """
    def __init__(self, itr, splits, source=0, parter=None, **kwds):
        super(LocalData, self).__init__(splits=splits, **kwds)
        self.fixed_source = source

        # One source and splits splits
        self._data = [bucket.WriteBucket(source, j, self.dir, self.format)
                for j in xrange(splits)]

        self._collect(itr, parter)
        self._data = [b.readonly_copy() for b in self._data]

    def __len__(self):
        """Number of buckets in this Dataset."""
        return self.splits

    def __iter__(self):
        """Iterate over all buckets."""
        return iter(self._data)

    def __setitem__(self, item, value):
        """Set an item."""
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        if part1 != self.fixed_source:
            raise ValueError('Source %s does not exist.' % part1)

        self._data[part2] = value

    def __getitem__(self, item):
        """Retrieve an item or split.

        At the moment, we're not very consistent about whether what we return
        is a view or a [shallow] copy.  Write at your own risk.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

        if isinstance(part1, slice):
            start, stop, step = part1.indices(self.fixed_source + 1)
            if self.fixed_source in xrange(start, stop, step):
                # Since the first part is a slice, we have to return a list.
                if isinstance(part2, slice):
                    return self._data[part2]
                else:
                    return [self._data[part2]]
            else:
                raise ValueError('Source not covered by the given range.')
        elif part1 != self.fixed_source:
            raise ValueError('Source %s does not exist.' % part1)
        else:
            return self._data[part2]

    def _collect(self, itr, parter):
        """Collect all of the key-value pairs from the given iterator."""
        buckets = list(self)
        n = self.splits
        if n == 1:
            bucket = buckets[0]
            bucket.collect(itr)
        else:
            for kvpair in itr:
                key, value = kvpair
                split = parter(key, n)
                bucket = buckets[split]
                bucket.addpair(kvpair)
        for bucket in buckets:
            bucket.close_writer()
        # Sync the containing dir to make sure the files are really written.
        if self.dir:
            fd = os.open(self.dir, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)


class RemoteData(Dataset):
    """A Dataset whose contents can be downloaded and read.

    Subclasses need to set the url for each bucket.
    """
    def __init__(self, **kwds):
        super(RemoteData, self).__init__(**kwds)

        self._fetched = False
        self._fetchlist_active = True
        self._init_fetchlist()
        self._close_callback = None

    def __getstate__(self):
        """Pickle without getting certain forbidden/unnecessary elements."""
        state = self.__dict__.copy()
        del state['_fetchlist']
        del state['_fetchlist_cv']
        state['_close_callback'] = None
        if self.closed:
            state['_data'] = None
        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._init_fetchlist()

    def _init_fetchlist(self):
        """Create the list of bucket ids (i.e., (source, split) pairs) that
        have newly available urls."""
        self._fetchlist = None
        self._fetchlist_cv = threading.Condition()

    def close(self):
        super(RemoteData, self).close()
        if self._close_callback:
            self._close_callback(self)

    # TODO: consider parallelizing this to use multiple downloading threads.
    def fetchall(self):
        """Download all of the files."""
        assert not self.closed, (
                'Invalid attempt to call fetchall on a closed dataset.')

        # Don't call fetchall twice:
        if self._fetched:
            return

        if not self._fetchlist_active:
            for bucket in self:
                url = bucket.url
                if url:
                    load.blocking_fill(url, bucket)
        else:
            more = True
            while more:
                with self._fetchlist_cv:
                    if self._fetchlist_active and not self._fetchlist:
                        self._fetchlist_cv.wait()
                    bucket_ids = self._fetchlist
                    self._fetchlist = []
                    more = self._fetchlist_active

                for key in bucket_ids:
                    bucket = self[key]
                    url = bucket.url
                    if url:
                        load.blocking_fill(url, bucket)

        self._fetched = True

    def notify_new_url(self, keylist):
        """Notify any fetchall method that new urls are available.

        The keylist parameter is an iterable of (source, split) pairs.
        """
        with self._fetchlist_cv:
            assert self._fetchlist_active
            self._fetchlist.extend(keylist)
            self._fetchlist_cv.notify_all()

    def notify_urls_known(self):
        """Signify that all buckets have been assigned urls.

        Notifies any fetchall method that all urls are known.
        """
        with self._fetchlist_cv:
            assert self._fetchlist_active
            self._fetchlist_active = False
            self._fetchlist_cv.notify_all()


class FileData(RemoteData):
    """A list of static files or urls to be used as input to an operation.

    For now, all of the files come from a single source, with one split for
    each file.

    >>> urls = ['http://aml.cs.byu.edu/', __file__]
    >>> data = FileData(urls)
    >>> len(data)
    2
    >>> data.fetchall()
    >>> data[0, 0][0]
    (0, '<html>\\n')
    >>> data[0, 0][1]
    (1, '<head>\\n')
    >>> data[0, 1][0]
    (0, '# Mrs\\n')
    >>>
    """
    def __init__(self, urls, sources=None, splits=None, **kwds):
        n = len(urls)

        if sources is None and splits is None:
            # Nothing specified, so we assume one split per url
            sources = 1
            splits = n
        elif sources is None:
            sources = n // splits
        elif splits is None:
            splits = n // sources

        # TODO: relax this requirement
        assert(sources * splits == n)

        super(FileData, self).__init__(sources=sources, splits=splits, **kwds)

        for bucket, url in izip(self, urls):
            bucket.url = url

        # Since all urls are pre-known, the fetchlist is unneeded.
        self._fetchlist_active = False


class ComputedData(RemoteData):
    """Manage input to or output from a map or reduce operation.

    The data are evaluated lazily.  A Dataset knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.

    Attributes:
        task_class: the class used to carry out computation
        func: name of the computing function (see registry for more info)
        parter: name of the partition function (see registry for more info)
    """
    def __init__(self, task_class, input, func_name, part_name=None, **kwds):
        # At least for now, we create 1 task for each split in the input
        ntasks = input.splits
        super(ComputedData, self).__init__(sources=ntasks, **kwds)

        self.task_class = task_class
        self.func_name = func_name
        self.part_name = part_name

        self.computed = False

        assert(not input.closed)
        self.input_id = input.id

    def run_serial(self, program, datasets):
        input_data = datasets[self.input_id]
        self.splits = 1
        func = getattr(program, self.func_name)
        parter = getattr(program, self.part_name)
        task = self.task_class(input_data, 0, 0, func, parter, self.splits,
                self.dir, self.format)
        task.run(serial=True)

        self._use_output(task.output)

        self.computed = True

    def fetchall(self):
        assert self.computed, (
                'Invalid attempt to call fetchall on a non-ready dataset.')
        super(ComputedData, self).fetchall()

    def _use_output(self, output):
        """Uses the contents of the given LocalData."""
        # Note that this assumes there's only one source and one output set.
        self._data = [output._data]
        self.sources = 1
        self.splits = len(output._data)
        self._fetched = True


def test():
    import doctest
    doctest.testmod()


class JunkToMoveSomewhereElse:
    def make_tasks(self):
        from task import MapTask
        for i in xrange(self.sources):
            # FIXME: self.input now refers to the dataset_id, not the dataset.
            map_task = MapTask(self.input, i, i, self.func,
                    self.parter, self.splits, self.dir, self.format)
            map_task.dataset = self
            self.tasks_todo.append(map_task)
        self.tasks_made = True

    def get_task(self):
        """Return the next available task"""
        if self.tasks_todo:
            task = self.tasks_todo.pop()
            return task
        else:
            return

    def task_started(self, task):
        self.tasks_active.append(task)

    def task_canceled(self, task):
        if task in self.tasks_active:
            self.tasks_active.remove(task)
            self.tasks_todo.append(task)
        elif task not in self.tasks_todo and task not in self.tasks_done:
            logger.warning('A task must be either todo, or active, or done.')

    def task_finished(self, task):
        if task in self.tasks_active:
            assert task not in self.tasks_done

            for bucket, url in izip(self[task.source, :], task.outurls()):
                bucket.url = url
            self.tasks_active.remove(task)
            self.tasks_done.append(task)
        else:
            if task in self.tasks_done:
                # someone else already did it
                logger.warning('Two slaves completed the same task.')
            else:
                # someone else already did it
                logger.warning('An inactive task was finished.')
            return

# vim: et sw=4 sts=4

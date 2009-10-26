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


# TODO: add a DataSet for resplitting input (right now we assume that input
# files are pre-split).

import os
from itertools import chain, izip

from logging import getLogger
logger = getLogger('mrs')


class BaseDataSet(object):
    """Manage input to or output from a map or reduce operation.

    A DataSet is naturally a two-dimensional list.  There are some number of
    sources, and for each source, there are one or more splits.

    Attributes:
        sources: number of input sources (e.g., tasks); to get all of the
            data from a particular source, use itersource()
        splits: number of outputs per source; to get all data for a particular
            split from all sources, use itersplit()
    """
    def __init__(self, sources=0, splits=0, dir=None, format=None,
            permanent=True):
        self.sources = sources
        self.splits = splits
        self.dir = dir
        self.format = format
        self.permanent = permanent
        self.closed = False
        self._data = None

    def __len__(self):
        """Number of buckets in this DataSet."""
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

    def close(self):
        """Close DataSet for future use.

        No additional DataSets will be able to depend on this DataSet for
        input, and no further reads will be allowed.  Calling close() allows
        the system to free resources.  Don't close a DataSet unless you really
        mean it.
        """
        self.closed = True
        if not self.permanent:
            from bucket import BucketRemover
            for bucket in self:
                self.blockingthread.register(BucketRemover(bucket))
            if self.dir:
                # Just to make sure it's all gone:
                from io.blocking import RecursiveRemover
                self.blockingthread.register(RecursiveRemover(self.dir))
        self._data = None

    def ready(self):
        """Report whether DataSet is ready.

        Ready means that the input DataSet is done(), so this DataSet can be
        computed without waiting.  For most types of DataSets, this is
        automatically true.
        """
        return True

    def done(self):
        """Report whether all data are accessible/computed.

        For most types of DataSets, this is automatically true.
        """
        return True

    def fetchall(self, *args, **kwds):
        """Download all of the files.

        For most types of DataSets, this is a no-op.
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
        self.close()


class DataSet(BaseDataSet):
    """Manage input to or output from a map or reduce operation.

    A DataSet is naturally a two-dimensional list.  There are some number of
    sources, and for each source, there are one or more splits.

    Low-level Testing.  Normally a DataSet holds Buckets, but for now we'll be
    loose for testing purposes.  This also makes it clear how slicing works,
    so it's not a waste of space.
    >>> ds = DataSet(sources=3, splits=3)
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
        BaseDataSet.__init__(self, **kwds)

        # For now assume that all sources have the same # of splits.  Also,
        # don't send the directory on to the Buckets because they shouldn't
        # be writing out to disk.
        from bucket import Bucket
        self._data = [[Bucket(i, j, format=self.format)
                for j in xrange(self.splits)]
                for i in xrange(self.sources)]

    def __len__(self):
        """Number of buckets in this DataSet."""
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


class LocalData(BaseDataSet):
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
        BaseDataSet.__init__(self, splits=splits, **kwds)
        self.fixed_source = source
        self.parter = parter

        # One source and splits splits
        from bucket import Bucket
        self._data = [Bucket(source, j, self.dir, self.format)
                for j in xrange(splits)]

        self._collect(itr)

    def __len__(self):
        """Number of buckets in this DataSet."""
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

    def _collect(self, itr):
        """Collect all of the key-value pairs from the given iterator."""
        buckets = list(self)
        n = self.splits
        if n == 1:
            bucket = buckets[0]
            bucket.collect(itr)
        else:
            parter = self.parter
            for kvpair in itr:
                key, value = kvpair
                split = parter(key, n)
                bucket = buckets[split]
                bucket.addpair(kvpair)
        for bucket in buckets:
            bucket.close_writer()
        # Sync the containing dir to make sure the files are really written.
        fd = os.open(self.dir, os.O_RDONLY)
        os.fsync(fd)
        os.close(fd)


class RemoteData(DataSet):
    """A DataSet whose contents can be downloaded and read.
    
    Subclasses need to set the url for each bucket.
    """
    def __init__(self, **kwds):
        DataSet.__init__(self, **kwds)

        self.blockingthread = None
        self._fetched = False
        # TODO: instead of needed_buckets and ready_buckets, just do a
        # defer.deferredList.
        self._needed_buckets = set()
        self._ready_buckets = set()

    def fetchall(self, serial=False):
        """Download all of the files.

        By default, fetchall assumes that it's being run in a thread other
        than the main thread because that's how it usually appears in Mrs.
        However, if it is in the main thread, it needs to know, so it can tell
        Twisted to catch SIGTERM.
        """
        from io.load import fillbucket, blocking_fill
        # Don't call fetchall twice:
        if self._fetched:
            return
        else:
            self._fetched = True

        if serial:
            for bucket in self:
                url = bucket.url
                if url:
                    blocking_fill(url, bucket)
        else:
            # TODO: set a maximum number of files to read at the same time (do
            # we really want to have 500 sockets open at once?)

            import threading
            from twisted.internet import reactor

            assert(not self.closed)

            self._download_done = threading.Condition()
            self._download_done.acquire()

            # TODO: It might be a good idea to make it so fetchall only tries
            # to load a particular split.  The reason is that mockparallel's
            # status report looks very confusing since the input for all
            # reduce tasks is being loaded at the beginning of the first
            # reduce task.
            # TODO: It might also make sense to use a DeferredList here.
            for bucket in self:
                url = bucket.url
                if url:

                    deferred = fillbucket(url, bucket, self.blockingthread)
                    reactor.callFromThread(deferred.addCallback,
                            self.callback, bucket)
                    self._needed_buckets.add(bucket)

            if self._needed_buckets:
                # block until all downloads finished
                self._download_done.wait()

            self._download_done.release()

    def callback(self, eof, bucket):
        """Called by Twisted when data are available for reading."""
        self._download_done.acquire()
        self._ready_buckets.add(bucket)
        if len(self._ready_buckets) == len(self._needed_buckets):
            self._download_done.notify()
            #from twisted.internet import reactor
            # Note that we can't do reactor.run() twice, so we cheat.
            #reactor.running = False
        self._download_done.release()

    def errback(self, value):
        # TODO: write me
        pass


class FileData(RemoteData):
    """A list of static files or urls to be used as input to an operation.

    For now, all of the files come from a single source, with one split for
    each file.

    >>> urls = ['http://aml.cs.byu.edu/', __file__]
    >>> data = FileData(urls)
    >>> len(data)
    2
    >>> data.fetchall(serial=True)
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
            sources = n / splits
        elif splits is None:
            splits = n / sources

        # TODO: relax this requirement
        assert(sources * splits == n)

        RemoteData.__init__(self, sources=sources, splits=splits, **kwds)

        for bucket, url in izip(self, urls):
            bucket.url = url


class ComputedData(RemoteData):
    """Manage input to or output from a map or reduce operation.
    
    The data are evaluated lazily.  A DataSet knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.
    """
    def __init__(self, input, func, splits, dir=None, parter=None,
            format=None, permanent=True):
        # At least for now, we create 1 task for each split in the input
        ntasks = input.splits
        RemoteData.__init__(self, sources=ntasks, splits=splits, dir=dir,
                permanent=permanent)

        self.func = func
        self.parter = parter

        self.tasks_made = False
        self.tasks_todo = []
        self.tasks_done = []
        self.tasks_active = []

        assert(not input.closed)
        self.input = input
        self.format = format

        # TODO: store a mapping from tasks to hosts and a map from hosts to
        # tasks.  This way you can know where to find data.  You also know
        # which hosts to restart in case of failure.

    def ready(self):
        """Report whether DataSet is ready to be computed.

        Ready means that the input DataSet is done(), so this DataSet can
        be computed without waiting.
        """
        if self.input:
            return self.input.done()
        else:
            return True

    def done(self):
        """Report whether everything has been computed.
        """
        if self.tasks_made and not self.tasks_todo and not self.tasks_active:
            return True
        else:
            return False

    def get_task(self):
        """Return the next available task"""
        if self.tasks_todo:
            task = self.tasks_todo.pop()
            return task
        else:
            return

    def status(self):
        active = len(self.tasks_active)
        todo = len(self.tasks_todo)
        done = len(self.tasks_done)
        total = active + todo + done
        return 'Completed: %s/%s, Active: %s' % (done, total, active)

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

    def close(self):
        """Close DataSet for future use.

        No additional DataSets will be able to depend on this DataSet for
        input, the data cannot be regenerated, and no further reads will be
        allowed.
        """
        super(ComputedData, self).close()
        self.input = None

    def _use_output(self, output):
        """Uses the contents of the given LocalData."""
        # Note that this assumes there's only one source and one output set.
        self._data = [output._data]
        self.sources = 1
        self.splits = len(output._data)
        self._fetched = True


class MapData(ComputedData):
    def make_tasks(self):
        from task import MapTask
        for i in xrange(self.sources):
            task = MapTask(self.input, i, i, self.func,
                    self.parter, self.splits, self.dir, self.format)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        from task import MapTask
        self.splits = 1
        task = MapTask(self.input, 0, 0, self.func, self.parter,
                self.splits, self.dir, self.format)
        # TODO: make this less hackish (this makes sure that done() works).
        self.tasks_todo = [None]
        self.tasks_made = True
        task.run(serial=True)
        self._use_output(task.output)
        self.tasks_todo = []


class ReduceData(ComputedData):
    def make_tasks(self):
        from task import ReduceTask
        for i in xrange(self.sources):
            task = ReduceTask(self.input, i, i, self.func,
                    self.parter, self.splits, self.dir, self.format)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        from task import ReduceTask
        self.splits = 1
        task = ReduceTask(self.input, 0, 0, self.func, self.parter,
                self.splits, self.dir, self.format)
        # TODO: make this less hackish (this makes sure that done() works).
        self.tasks_todo = [None]
        self.tasks_made = True
        task.run(serial=True)
        self._use_output(task.output)
        self.tasks_todo = []


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

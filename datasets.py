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


# TODO: right now we assume that input files are pre-split.

from heapq import heappush
from itertools import chain, izip
import os

from io import HexWriter, fillbucket


# TODO: cache data to disk when memory usage is high
class Bucket(object):
    """Hold data from a source or for a split.

    Data can be manually dumped to disk, in which case the data will be saved
    to the given filename with the specified format.  Eventually Bucket will
    be upgraded to automatically cache data to disk if they get too large to
    stay in memory.

    If (bucket.heap == True), then store data in a heap as they arrive (which
    makes subsequent sorting faster).

    >>> b = Bucket()
    >>> b.append((4, 'test'))
    >>> b.collect([(3, 'a'), (1, 'This'), (2, 'is')])
    >>> ' '.join(value for key, value in b)
    'test a This is'
    >>> b.sort()
    >>> ' '.join(value for key, value in b)
    'This is a test'
    >>>
    """
    def __init__(self, filename=None, format=HexWriter):
        self._data = []
        self.heap = False
        self.format = format
        self.filename = filename
        self.url = None

    def append(self, x):
        """Collect a single key-value pair
        """
        if self.sort:
            heappush(self._data, x)
        else:
            self._data.append(x)

    def collect(self, pairiter):
        """Collect all key-value pairs from the given iterable

        The collection can be a generator or a Mrs format.  This will block if
        the iterator blocks.
        """
        data = self._data
        if self.heap:
            for kvpair in pairiter:
                heappush(data, kvpair)
                data.append(kvpair)
        else:
            for kvpair in pairiter:
                data.append(kvpair)

    def sort(self):
        self._data.sort()

    def clear(self):
        """Remove all key-value pairs from the Bucket."""
        self._data = []

    def __len__(self):
        return len(self._data)

    def __getitem__(self, item):
        """Get a particular item, mainly for debugging purposes"""
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    # TODO: write doctest for dump
    def dump(self, directory):
        assert(self.filename is not None)
        path = os.path.join(directory, self.filename)
        if len(self):
            f = open(path, 'w')
            writer = self.format(f)
            for key, value in self:
                writer.writepair(key, value)
            f.close()


class DataSet(object):
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
    def __init__(self, sources=0, splits=0, directory=None, format=HexWriter):
        self.directory = directory
        self.sources = sources
        self.splits = splits
        self.format = format
        self.closed = False

        # Whether self.directory will get cleaned up
        self.temporary = not self.directory

        # For now assume that all sources have the same # of splits.
        self._data = [[Bucket(filename=self.filename(i, j))
                for j in xrange(splits)]
                for i in xrange(sources)]

    def __len__(self):
        """Number of buckets in this DataSet."""
        return sum(len(source) for source in self._data)

    def __iter__(self):
        """Iterate over all buckets."""
        return chain(*self._data)

    def itersplit(self, split):
        buckets = self[:, split]
        return chain(*buckets)

    def itersource(self, source):
        buckets = self[source, :]
        return chain(*buckets)

    # TODO: remove temporary directories!!
    def close(self):
        """Close DataSet for future use.

        No additional DataSets will be able to depend on this DataSet for
        input, and no further reads will be allowed.  Calling close() allows
        the system to free resources.  Don't close a DataSet unless you really
        mean it.
        """
        if self.directory and self.temporary:
            from util import remove_recursive
            remove_recursive(self.directory)
        self.closed = True

    def dump(self):
        """Write out all of the key-value pairs to files."""
        import tempfile
        # TODO: we should really clean these directories up at some point!
        if self.directory is None:
            self.directory = tempfile.mkdtemp()
        for source in self._data:
            for bucket in source:
                bucket.dump(self.directory)

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

    def fetchall(self, **kwds):
        """Download all of the files.

        For most types of DataSets, this is a no-op.
        """
        return

    def filename(self, source, split):
        """Return the filename for the output split for the given index.
        
        >>> ds = DataSet(sources=4, splits=5, directory='/tmp')
        >>> ds.filename(2, 4)
        '/tmp/source_2_split_4.mrsx'
        >>>
        """
        return 'source_%s_split_%s.%s' % (source, split, self.format.ext)

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

        isslice1 = isinstance(part1, slice)
        isslice2 = isinstance(part1, slice)

        data = self._data
        if isslice1:
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


class Output(DataSet):
    """Collect output from a map or reduce task.
    
    This is only used on the slave side.  It takes a partition function and a
    number of splits to use.  Note that the `source`, which is just used for
    naming files, represents which output source is being created.

    >>> o = Output((lambda x, n: x%n), 4)
    >>> lst = [(4, 'to_0'), (5, 'to_1'), (7, 'to_3'), (9, 'to_1')]
    >>> o.collect(lst)
    >>> list(o[0, 1])
    [(5, 'to_1'), (9, 'to_1')]
    >>> list(o[0, 3])
    [(7, 'to_3')]
    >>>
    """
    def __init__(self, partition, splits, source=0, **kwds):
        super(Output, self).__init__(splits=splits, **kwds)

        self.partition = partition
        # One source and splits splits
        self._data = [[Bucket(format=self.format,
                filename=self.filename(source, i)) for i in xrange(splits)]]

        # For now, the externally visible url is just the filename on the
        # local or networked filesystem.
        for bucket in self:
            bucket.url = bucket.filename

    def collect(self, itr):
        """Collect all of the key-value pairs from the given iterator."""
        buckets = self[0, :]
        n = self.splits
        if n == 1:
            bucket = buckets[0]
            bucket.collect(itr)
        else:
            partition = self.partition
            for kvpair in itr:
                key, value = kvpair
                split = partition(key, n)
                bucket = buckets[split]
                bucket.append(kvpair)


class RemoteData(DataSet):
    """A DataSet whose contents can be downloaded and read.
    
    Subclasses need to set the url for each bucket.
    """
    def __init__(self, **kwds):
        super(RemoteData, self).__init__(**kwds)

        self.blockingthread = None
        self._fetched = False
        # TODO: instead of needed_buckets and ready_buckets, just do a
        # defer.deferredList.
        self._needed_buckets = set()
        self._ready_buckets = set()

    def fetchall(self, serial=False, heap=False):
        """Download all of the files.

        By default, fetchall assumes that it's being run in a thread other
        than the main thread because that's how it usually appears in Mrs.
        However, if it is in the main thread, it needs to know, so it can tell
        Twisted to catch SIGTERM.
        """
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

            if heap:
                for bucket in self:
                    bucket.heap = True

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
    >>> data.fetchall(mainthread=True)
    >>> data[0, 0][0]
    (0, '<html>\\n')
    >>> data[0, 0][1]
    (1, '<head>\\n')
    >>> data[0, 1][0]
    (0, '#!/usr/bin/env python\\n')
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

        super(FileData, self).__init__(sources=sources, splits=splits, **kwds)

        from itertools import izip
        for bucket, url in zip(self, urls):
            bucket.url = url


class ComputedData(RemoteData):
    """Manage input to or output from a map or reduce operation.
    
    The data are evaluated lazily.  A DataSet knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.
    """
    def __init__(self, input, func, nparts, outdir, parter=None,
            format=None, registry=None):
        # At least for now, we create 1 task for each split in the input
        ntasks = input.splits
        super(ComputedData, self).__init__(sources=ntasks, splits=nparts)

        if registry is None:
            from registry import Registry
            self.registry = Registry()
        else:
            self.registry = registry

        self.func_name = self.registry.as_name(func)
        if parter is None:
            self.part_name = ''
        else:
            self.part_name = self.registry.as_name(parter)

        self.tasks_made = False
        self.tasks_todo = []
        self.tasks_done = []
        self.tasks_active = []

        assert(not input.closed)
        self.input = input
        self.outdir = outdir
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
        self.tasks_active.remove(task)
        self.tasks_todo.append(task)

    def task_finished(self, task):
        if task in self.tasks_active:
            assert task not in self.tasks_done

            for bucket, url in izip(self[task.source, :], task.outurls()):
                bucket.url = url
            self.tasks_active.remove(task)
            self.tasks_done.append(task)
        else:
            import sys
            if task in self.tasks_done:
                # someone else already did it
                print >>sys.stderr, "Warning: two slaves completed the same task!"
            else:
                # someone else already did it
                print >>sys.stderr, "Warning: an inactive task was finished!"
            return

    def close(self):
        """Close DataSet for future use.

        No additional DataSets will be able to depend on this DataSet for
        input, the data cannot be regenerated, and no further reads will be
        allowed.
        """
        self.closed = True
        self.input = None

    def _use_output(self, output):
        """Uses the contents of the given Output dataset."""
        self._data = output._data
        self._fetched = True


class MapData(ComputedData):
    def make_tasks(self):
        from task import MapTask
        for i in xrange(self.sources):
            task = MapTask(self.input, i, i, self.func_name,
                    self.part_name, self.splits, self.outdir, self.format,
                    self.registry)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        from task import MapTask
        self.splits = 1
        task = MapTask(self.input, 0, 0, self.func_name, self.part_name,
                self.splits, self.outdir, self.format, self.registry)
        self.tasks_made = True
        task.run(serial=True)


class ReduceData(ComputedData):
    def make_tasks(self):
        from task import ReduceTask
        for i in xrange(self.sources):
            task = ReduceTask(self.input, i, i, self.func_name,
                    self.part_name, self.splits, self.outdir, self.format,
                    self.registry)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        from task import ReduceTask
        self.splits = 1
        task = ReduceTask(self.input, 0, 0, self.func_name, self.part_name,
                self.splits, self.outdir, self.format, self.registry)
        self.tasks_made = True
        task.run(serial=True)


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

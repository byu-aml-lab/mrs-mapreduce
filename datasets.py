#!/usr/bin/env python

# 3rd TODO: make it all work

# TODO: right now we assume that input files are pre-split.

import threading, os
from heapq import heappush
from itertools import chain, izip

from io import fileformat, openreader, HexWriter


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
    >>> ' '.join([value for key, value in b])
    'test a This is'
    >>> b.sort()
    >>> ' '.join([value for key, value in b])
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

    def __getitem__(self, item):
        """Get a particular item, mainly for debugging purposes"""
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    # TODO: write doctest for dump
    def dump(self):
        assert(self.filename is not None)
        f = open(self.filename, 'w')
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
        if directory is None:
            from tempfile import mkdtemp
            self.directory = mkdtemp()
            self.temp = True
        else:
            self.directory = directory
            self.temp = False

        self.sources = sources
        self.splits = splits
        self.format = format

        # For now assume that all sources have the same # of splits.
        self._data = [[Bucket(filename=self.path(i, j))
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

    def dump(self):
        """Write out all of the key-value pairs to files."""
        for source in self._data:
            for bucket in source:
                bucket.dump()

    def close(self):
        if self.temp:
            os.removedirs(self.directory)

    def path(self, source, split):
        """Return the path to the output split for the given index.
        
        >>> ds = DataSet(sources=4, splits=5, directory='/tmp')
        >>> ds.path(2, 4)
        '/tmp/source_2_split_4.mrsx'
        >>>
        """
        filename = "source_%s_split_%s.%s" % (source, split, self.format.ext)
        return os.path.join(self.directory, filename)

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
    number of splits to use.

    >>> o = Output((lambda x, n: x%n), 4)
    >>> lst = [(4, 'to_0'), (5, 'to_1'), (7, 'to_3'), (9, 'to_1')]
    >>> o.collect(lst)
    >>> list(o[0, 1])
    [(5, 'to_1'), (9, 'to_1')]
    >>> list(o[0, 3])
    [(7, 'to_3')]
    >>>
    """
    def __init__(self, partition, splits, taskid=0, **kwds):
        super(Output, self).__init__(splits=splits, **kwds)

        self.partition = partition
        # One source and splits splits
        self._data = [[Bucket(format=self.format,
                filename=self.path(taskid, i)) for i in xrange(splits)]]

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


class FileData(DataSet):
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

        self.ready_buckets = set()

    #def fetchall(self, mainthread=False):
    def fetchall(self, heap=False):
        """Download all of the files

        By default, fetchall assumes that it's being run in a thread other
        than the main thread because that's how it usually appears in Mrs.
        However, if it is in the main thread, it needs to know, so it can tell
        Twisted to catch SIGTERM.
        """
        # TODO: set a maximum number of files to read at the same time (do we
        # really want to have 500 sockets open at once?)

        if heap:
            for bucket in self:
                bucket.heap = True

        for bucket in self:
            reader = openreader(bucket.url)
            reader.buf.deferred.addCallback(self.callback, bucket, reader)

        from twisted.internet import reactor
        # Note that we can't do reactor.run() twice, so we cheat.
        #reactor.run()
        #reactor.run(installSignalHandlers=0)
        reactor.running = True
        reactor.mainLoop()

    def callback(self, eof, bucket, reader):
        """Called by Twisted when data are available for reading."""
        bucket.collect(reader)
        #import sys
        #print >>sys.stderr, "Got data"
        if eof:
            #print >>sys.stderr, "EOF"
            self.ready_buckets.add(bucket)
            if len(self.ready_buckets) == len(self):
                from twisted.internet import reactor
                # Note that we can't do reactor.run() twice, so we cheat.
                #reactor.stop()
                reactor.running = False

    def errback(self, value):
        # TODO: write me
        pass


class ComputedData(DataSet):
    """Manage input to or output from a map or reduce operation.
    
    The data are evaluated lazily.  A DataSet knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.
    """
    def __init__(self, input, func, nparts, outdir, parter=None,
            registry=None):
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

        self.input = input
        self.outdir = outdir

        # TODO: store a mapping from tasks to hosts and a map from hosts to
        # tasks.  This way you can know where to find data.  You also know
        # which hosts to restart in case of failure.

    def ready(self):
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

    def print_status(self):
        active = len(self.tasks_active)
        todo = len(self.tasks_todo)
        done = len(self.tasks_done)
        total = active + todo + done
        print 'Completed: %s/%s, Active: %s' % (done, total, active)

    def task_started(self, task):
        self.tasks_active.append(task)

    def task_canceled(self, task):
        self.tasks_active.remove(task)
        self.tasks_todo.append(task)

    def task_finished(self, task):
        for bucket, url in izip(self[task.taskid, :], task.outurls()):
            bucket.url = url
        self.tasks_active.remove(task)
        self.tasks_done.append(task)


class MapData(ComputedData):
    def __init__(self, input, mapper, nparts, outdir, parter=None,
            registry=None):
        ComputedData.__init__(self, input, mapper, nparts, outdir,
                parter=None, registry=registry)

    def make_tasks(self):
        from mapreduce import MapTask
        for taskid in xrange(self.sources):
            task = MapTask(taskid, self.input, self.registry, self.func_name,
                    self.part_name, self.outdir, self.splits)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        pass
        #input_files = [io.openfile(filename) for filename in self.inputs]
        #all_input = chain(*input_files)
        #map_output = mrs_map(registry['mapper'], all_input)


class ReduceData(ComputedData):
    def __init__(self, input, reducer, nparts, outdir, parter=None,
            registry=None):
        ComputedData.__init__(self, input, reducer, nparts, outdir,
                parter=None, registry=registry)

    def make_tasks(self):
        from mapreduce import ReduceTask
        for taskid in xrange(self.sources):
            task = ReduceTask(taskid, self.input, self.registry,
                    self.func_name, self.outdir)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True


def test():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test()

# vim: et sw=4 sts=4

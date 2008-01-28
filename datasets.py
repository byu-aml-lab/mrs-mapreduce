#!/usr/bin/env python

# 1st TODO: FileData
# 2nd TODO: unify Input, Output, and Computed: api for sources, splits, etc.
# 3rd TODO: make it all work

# TODO: right now we assume that input files are pre-split.

import threading, os
from heapq import heappush

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
        self.filename = filename

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

    def __getitem__(self, item):
        """Get a particular item, mainly for debugging purposes"""
        return self._data[item]

    def __iter__(self):
        return iter(self._data)

    # TODO: write doctest for dump
    def dump(self):
        assert(self.filename is not None)
        f = openfile(self.filename, 'w')
        for key, value in self:
            f.write(key, value)
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
    3
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

        # TODO: set filename for each Bucket upon creation

        # For now assume that all sources have the same # of splits.
        self._data = [[Bucket() for j in xrange(splits)]
                for i in xrange(sources)]

    def __setitem__(self, item, value):
        """Set an item.

        For now, you can't set a split.
        """
        # Separate the two dimensions:
        try:
            part1, part2 = item
        except (TypeError, ValueError):
            raise TypeError("Requires a pair of items.")

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

    def __len__(self):
        """Number of sources in this DataSet."""
        return len(self._data)


class Output(DataSet):
    """Collect output from a map or reduce task."""
    def __init__(self, partition, nsplits, **kwds):
        super(Output, self).__init__(**kwds)

        self.partition = partition
        # One source and nsplits splits
        self._data = [[Bucket(format=format, filename=self.path(i))
                for i in xrange(nsplits)]]

    def close(self):
        if self.temp:
            os.removedirs(self.directory)

    def collect(self, itr):
        """Collect all of the key-value pairs from the given iterator."""
        buckets = self[0, :]
        n = self.nsplits
        if n == 1:
            bucket = self.splits[0]
            bucket.collect(itr)
        else:
            partition = self.partition
            nsplits = self.nsplits
            for kvpair in itr:
                key, value = kvpair
                split = partition(key, nsplits)
                bucket = self.splits[split]
                bucket.append(kvpair)

    def path(self, index):
        """Return the path to the output split for the given index."""
        filename = "split_%s.%s" % (index, self.format)
        return os.path.join(self.directory, filename)

    def savetodisk(self):
        """Write out all of the key-value pairs to files."""
        for bucket in self.splits:
            bucket.dump()


class FileData(DataSet):
    """A list of static files or urls to be used as input to an operation.

    #>>> urls = ['/etc/passwd', '/etc/group']
    >>> urls = ['http://google.com/', '/etc/passwd']
    >>> data = FileData(urls)
    >>> len(data)
    2
    >>> data.fetchall()
    >>> data[0, 0][0]
    >>> data[1, 0][0]
    >>>
    """
    def __init__(self, urls):
        sources = len(urls)
        self._urls = tuple(urls)
        self.ready_buckets = set()

        super(FileData, self).__init__(sources=sources, splits=1)

    def fetchall(self):
        """Download all of the files
        """
        # TODO: set a maximum number of files to read at the same time (do we
        # really want to have 500 sockets open at once?)

        # TODO: setup io.net.download for
        # each url, and run a twisted reactor loop to download it all
        for i, url in enumerate(self._urls):
            reader = openreader(url)
            bucket = self[i, 0]
            reader.buf.deferred.addCallback(self.callback, bucket, reader)

        from twisted.internet import reactor
        reactor.run()

    def callback(self, eof, bucket, reader):
        """Called by Twisted when data are available for reading."""
        import sys
        bucket.collect(reader)
        print >>sys.stderr, "Got data"
        if eof:
            print >>sys.stderr, "EOF"
            self.ready_buckets.add(bucket)
            if len(self.ready_buckets) == len(self):
                from twisted.internet import reactor
                reactor.stop()

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
        self.input = input
        self.ntasks = len(self.input)
        self.outdir = outdir
        if registry is None:
            from registry import Registry
            self.registry = Registry()
        else:
            self.registry = registry
        self.nparts = nparts

        self.func_name = self.registry.as_name(func)
        if parter is None:
            self.part_name = ''
        else:
            self.part_name = self.registry.as_name(parter)

        self.tasks_made = False
        self.tasks_todo = []
        self.tasks_done = []
        self.tasks_active = []

        # TODO: store a mapping from tasks to hosts and a map from hosts to
        # tasks.  This way you can know where to find data.  You also know
        # which hosts to restart in case of failure.

    def __len__(self):
        return self.nparts

    def __getitem__(self, split):
        if not self.ready():
            raise IndexError
        else:
            return [task.outurls[split] for task in self.tasks_done
                    if task.outurls[split] != '']

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


class MapData(ComputedData):
    def __init__(self, input, mapper, nparts, outdir, parter=None,
            registry=None):
        ComputedData.__init__(self, input, mapper, nparts, outdir,
                parter=None, registry=registry)

    def make_tasks(self):
        from mapreduce import MapTask
        for taskid in xrange(self.ntasks):
            task = MapTask(taskid, self.input, self.registry, self.func_name,
                    self.part_name, self.outdir, self.nparts)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True

    def run_serial(self):
        pass
        input_files = [io.openfile(filename) for filename in self.inputs]
        all_input = chain(*input_files)
        map_output = mrs_map(registry['mapper'], all_input)


class ReduceData(ComputedData):
    def __init__(self, input, reducer, nparts, outdir, parter=None,
            registry=None):
        ComputedData.__init__(self, input, reducer, nparts, outdir,
                parter=None, registry=registry)

    def make_tasks(self):
        from mapreduce import ReduceTask
        for taskid in xrange(self.ntasks):
            task = ReduceTask(taskid, self.input, self.registry,
                    self.func_name, self.outdir)
            task.dataset = self
            self.tasks_todo.append(task)
        self.tasks_made = True


def test_datasets():
    import doctest
    doctest.testmod()

if __name__ == "__main__":
    test_datasets()

# vim: et sw=4 sts=4

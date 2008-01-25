#!/usr/bin/env python

# TODO: right now we assume that input files are pre-split.

import threading
from heapq import heappush

from io import fileformat, HexFormat


# TODO: cache data to disk when memory usage is high
class Bucket(object):
    """Hold data from a source or for a split.

    Data can be manually dumped to disk, in which case the data will be saved
    to the given filename with the specified format.  Eventually Bucket will
    be upgraded to automatically cache data to disk if they get too large to
    stay in memory.

    >>> b = Bucket()
    >>> b.append((4, 'test'))
    >>> b.collect([(3, 'a'), (1, 'This'), (2, 'is')])
    >>> ' '.join([value for key, value in b])
    'test a This is'
    >>> b.sort = True
    >>> ' '.join([value for key, value in b])
    'This is a test'
    >>>
    """
    def __init__(self, sort=False, filename=None, format=HexFormat):
        self._data = []
        self.sort = sort

        self.ready = False

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
        if self.sort:
            for kvpair in pairiter:
                heappush(data, kvpair)
                data.append(kvpair)
        else:
            for kvpair in pairiter:
                data.append(kvpair)

    def __iter__(self):
        if self.sort:
            return iter(sorted(self._data))
        else:
            return iter(self._data)


class DataSet(object):
    """Manage input to or output from a map or reduce operation.
    """
    def __len__(self):
        """Number of splits in this DataSet."""
        return 0

    def __getitem__(self, split):
        """Retrieve a seq of URLs for a particular split in this DataSet."""
        raise IndexError

    def ready(self, split=None):
        """Whether or not a split is ready to be retrieved.

        If the split is not specified, ready() tells whether or not
        all splits are ready.
        """
        return True


class FileData(DataSet):
    """A list of static files that can be used as input to an operation.

    >>> urls = ['http://aml.cs.byu.edu/', 'http://www.cs.byu.edu/']
    >>> data = FileData(urls)
    >>> len(data)
    2
    >>> data[1]
    'http://www.cs.byu.edu/'
    >>>
    """
    def __init__(self, urls):
        self._len = len(urls)
        self._urls = tuple(urls)

    def __len__(self):
        """How many files are in the DataSet."""
        return self._len

    def __getitem__(self, i):
        """Retrieve a sequence containing the URL for the specified file."""
        return (self._urls[i],)


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

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

"""MapReduce Tasks.

A Task represents a unit of work and the mechanism for carrying it out.
"""

from itertools import chain

from . import datasets
from . import fileformats


class Task(object):
    """Manage input and output for a piece of a map or reduce operation.

    The `task_index` determines the split of the input dataset that will be
    used by this Task, as well as the source number that will be created by
    this Task.
    """
    def __init__(self, input, task_index, storage, format):
        self.input = input
        self.source = task_index
        self.split = task_index
        self.storage = storage
        self.dataset = None
        if format is None:
            format = fileformats.default_write_format
        self.format = format
        self.output = None

    def outurls(self):
        return [(b.split, b.url) for b in self.output if len(b)]


class MapTask(Task):
    def __init__(self, input, task_index, mapper, parter, splits, storage,
            format):
        Task.__init__(self, input, task_index, storage, format)
        self.mapper = mapper
        self.partition = parter
        self.splits = splits

    def run(self, serial=False):
        # SETUP INPUT
        self.input.fetchall()
        if serial:
            all_input = self.input.iterdata()
        else:
            all_input = self.input.itersplit(self.split)

        if self.storage and (self.splits > 1):
            import tempfile
            prefix = 'source_%s_' % self.source
            subdir = tempfile.mkdtemp(prefix=prefix, dir=self.storage)
        else:
            subdir = self.storage

        # MAP PHASE
        itr = self.map(all_input)
        self.output = datasets.LocalData(itr, self.splits, source=self.source,
                parter=self.partition, dir=subdir, format=self.format)

    def map(self, input):
        """Yields map output iterating over the entries in input."""
        for inkey, invalue in input:
            for key, value in self.mapper(inkey, invalue):
                yield (key, value)


class ReduceTask(Task):
    def __init__(self, input, task_index, reducer, parter, splits, storage,
            format):
        Task.__init__(self, input, task_index, storage, format)
        self.reducer = reducer
        self.partition = parter
        self.splits = splits

    def run(self, serial=False):
        # SORT PHASE
        self.input.fetchall()
        if serial:
            all_input = self.input.iterdata()
        else:
            all_input = self.input.itersplit(self.split)
        sorted_input = sorted(all_input)

        if self.storage and (self.splits > 1):
            import tempfile
            prefix = 'source_%s_' % self.source
            subdir = tempfile.mkdtemp(prefix=prefix, dir=self.storage)
        else:
            subdir = self.storage

        # Do the following if external sort is necessary (i.e., the input
        # files are too big to fit in memory):
        #fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        #os.close(fd)
        #io.hexfile_sort(interm_names, sorted_name)

        # REDUCE PHASE
        itr = self.reduce(sorted_input)
        self.output = datasets.LocalData(itr, self.splits, source=self.source,
                parter=self.partition, dir=subdir, format=self.format)

    def reduce(self, input):
        """Yields reduce output iterating over the entries in input.

        A reducer is an iterator taking a key and an iterator over values for
        that key.  It yields values for that key.
        """
        for key, iterator in self.grouped_read(input):
            for value in self.reducer(key, iterator):
                yield (key, value)

    def grouped_read(self, input_file):
        """Yields key-iterator pairs over a sorted input_file.

        This is very similar to itertools.groupby, except that we assume that
        the input_file is sorted, and we assume key-value pairs.
        """
        input_itr = iter(input_file)
        input = input_itr.next()
        next_pair = list(input)

        def subiterator():
            # Closure warning: don't rebind next_pair anywhere in this function
            group_key, value = next_pair

            while True:
                yield value
                try:
                    input = input_itr.next()
                except StopIteration:
                    next_pair[0] = None
                    return
                key, value = input
                if key != group_key:
                    # A new key has appeared.
                    next_pair[:] = key, value
                    return

        while next_pair[0] is not None:
            yield next_pair[0], subiterator()
        raise StopIteration


class ReduceMapTask(MapTask, ReduceTask):
    def __init__(self, input, task_index, reducer, mapper, parter, splits,
            storage, format):
        Task.__init__(self, input, task_index, storage, format)
        self.reducer = reducer
        self.mapper = mapper
        self.partition = parter
        self.splits = splits

    def run(self, serial=False):
        # SETUP INPUT
        self.input.fetchall()
        if serial:
            all_input = self.input.iterdata()
        else:
            all_input = self.input.itersplit(self.split)
        sorted_input = sorted(all_input)

        if self.storage and (self.splits > 1):
            import tempfile
            prefix = 'source_%s_' % self.source
            subdir = tempfile.mkdtemp(prefix=prefix, dir=self.storage)
        else:
            subdir = self.storage

        # REDUCEMAP PHASE
        itr = self.map(self.reduce(sorted_input))
        self.output = datasets.LocalData(itr, self.splits, source=self.source,
                parter=self.partition, dir=subdir, format=self.format)


class Operation(object):
    def __init__(self, **kwds):
        self.part_name = kwds.get('part_name', None)


class MapOperation(Operation):
    def __init__(self, **kwds):
        super(MapOperation, self).__init__(**kwds)
        self.map_name = kwds.get('map_name', None)
        self.id = '%s' % self.map_name

    def make_task(self, program, input_data, task_index, splits, out_dir,
            out_format):
        if self.map_name is None:
            mapper = None
        else:
            mapper = getattr(program, self.map_name)
        parter = getattr(program, self.part_name)

        return MapTask(input_data, task_index, mapper, parter, splits,
                out_dir, out_format)


class ReduceOperation(Operation):
    def __init__(self, **kwds):
        super(ReduceOperation, self).__init__(**kwds)
        self.reduce_name = kwds.get('reduce_name', None)
        self.id = '%s' % self.reduce_name

    def make_task(self, program, input_data, task_index, splits, out_dir,
            out_format):
        if self.reduce_name is None:
            reducer = None
        else:
            reducer = getattr(program, self.reduce_name)
        parter = getattr(program, self.part_name)

        return ReduceTask(input_data, task_index, reducer, parter, splits,
                out_dir, out_format)


class ReduceMapOperation(Operation):
    def __init__(self, **kwds):
        super(ReduceMapOperation, self).__init__(**kwds)
        self.reduce_name = kwds.get('reduce_name', None)
        self.map_name = kwds.get('map_name', None)
        self.id = '%s_%s' % (self.reduce_name, self.map_name)

    def make_task(self, program, input_data, task_index, splits, out_dir,
            out_format):
        if self.reduce_name is None:
            reducer = None
        else:
            reducer = getattr(program, self.reduce_name)
        if self.map_name is None:
            mapper = None
        else:
            mapper = getattr(program, self.map_name)
        parter = getattr(program, self.part_name)

        return ReduceMapTask(input_data, task_index, reducer, mapper,
                parter, splits, out_dir, out_format)


# vim: et sw=4 sts=4

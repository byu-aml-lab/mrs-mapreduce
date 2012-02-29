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

from . import datasets
from . import fileformats
from . import util


class Task(object):
    """Manage input and output for a piece of a map or reduce operation.

    The `task_index` determines the split of the input dataset that will be
    used by this Task, as well as the source number that will be created by
    this Task.
    """
    def __init__(self, op, input_ds, dataset_id, task_index, splits, storage,
            ext):
        self.op = op
        self.input_ds = input_ds
        self.dataset_id = dataset_id
        self.task_index = task_index
        self.splits = splits
        self.storage = storage if (storage is not None) else ''
        self.ext = ext

        self.outdir = None
        self.output = None

    def outurls(self):
        return [(b.split, b.url) for b in self.output[:, :] if len(b)]

    @staticmethod
    def from_op(op, *args):
        return op.task_class(op, *args)

    @staticmethod
    def from_args(op_args, urls, dataset_id, task_index, splits, storage,
            ext):
        """Converts from a simple tuple to a Task.

        The elements of the tuple correspond to the arguments of the
        Task.__init__ method, with the difference that the first argument
        is an Operation args tuple, and the second is a list of urls.
        """
        op = Operation.from_args(*op_args)
        input_ds = datasets.FileData(urls, splits=1, first_split=task_index)
        return Task.from_op(op, input_ds, dataset_id, task_index, splits,
                storage, ext)

    def to_args(self):
        """Converts the Task to a simple tuple.

        The elements of the tuple correspond to arguments of the Task.__init__
        method.  The first two elements of the tuples are lists of strings.
        The first is a list-of-strings representation of an operation, and the
        second is a list of urls.  The remaining elements are identical
        to the corresponding elements of the init method.
        """
        op_args = self.op.to_args()
        urls = [b.url for b in self.input_ds[:, self.task_index] if b.url]
        return (op_args, urls, self.dataset_id, self.task_index,
                self.splits, self.storage, self.ext)

    def make_outdir(self, default_dir):
        """Makes an output directory if necessary.

        Returns a bool indicating whether the files should be preserved (as
        opposed to automatically deleted).  Sets self.outdir, a path to a
        directory where output files should be created.
        """
        permanent = False
        if self.storage or default_dir:
            if self.storage:
                self.outdir = self.storage
                permanent = True
            else:
                self.outdir = util.mktempdir(default_dir, self.dataset_id + '_')

            if self.splits > 1:
                prefix = 'source_%s_' % self.task_index
                self.outdir = util.mktempdir(self.outdir, prefix)
        return permanent

    def format(self):
        """Return the write format given the Task's file extension."""
        if self.ext:
            format = fileformats.writerformat(self.ext)
        else:
            format = fileformats.default_write_format


class MapTask(Task):
    def run(self, program, default_dir, serial=False):
        assert isinstance(self.op, MapOperation)

        # SETUP INPUT
        self.input_ds.fetchall()
        if serial:
            all_input = self.input_ds.data()
        else:
            all_input = self.input_ds.splitdata(self.task_index)

        # SETUP OUTPUT
        permanent = self.make_outdir(default_dir)

        # MAP PHASE
        map_itr = self.op.map(program, all_input)
        self.output = datasets.LocalData(map_itr, self.splits,
                source=self.task_index, parter=self.op.parter(program),
                dir=self.outdir, format=self.format(), permanent=permanent)


class ReduceTask(Task):
    def run(self, program, default_dir, serial=False):
        assert isinstance(self.op, ReduceOperation)

        # SETUP INPUT
        self.input_ds.fetchall()
        if serial:
            all_input = self.input_ds.data()
        else:
            all_input = self.input_ds.splitdata(self.task_index)

        # SORT PHASE
        sorted_input = sorted(all_input)

        # SETUP OUTPUT
        permanent = self.make_outdir(default_dir)

        # REDUCE PHASE
        reduce_itr = self.op.reduce(program, sorted_input)
        self.output = datasets.LocalData(reduce_itr, self.splits,
                source=self.task_index, parter=self.op.parter(program),
                dir=self.outdir, format=self.format(), permanent=permanent)


class ReduceMapTask(Task):
    def run(self, program, default_dir, serial=False):
        assert isinstance(self.op, ReduceMapOperation)

        # SETUP INPUT
        self.input_ds.fetchall()
        if serial:
            all_input = self.input_ds.data()
        else:
            all_input = self.input_ds.splitdata(self.task_index)

        # SORT PHASE
        sorted_input = sorted(all_input)

        # SETUP OUTPUT
        permanent = self.make_outdir(default_dir)

        # REDUCEMAP PHASE
        reduce_itr = self.op.reduce(program, sorted_input)
        map_itr = self.op.map(program, reduce_itr)
        self.output = datasets.LocalData(map_itr, self.splits,
                source=self.task_index, parter=self.op.parter(program),
                dir=self.outdir, format=self.format(), permanent=permanent)


class Operation(object):
    def __init__(self, part_name):
        self.part_name = part_name

    def parter(self, program):
        return getattr(program, self.part_name)

    @staticmethod
    def from_args(op_name, *args):
        cls = OP_CLASSES[op_name]
        return cls(*args)


class MapOperation(Operation):
    op_name = 'map'
    task_class = MapTask

    def __init__(self, map_name, *args):
        Operation.__init__(self, *args)
        self.map_name = map_name
        self.id = '%s' % self.map_name

    def map(self, program, input):
        """Yields map output iterating over the entries in input."""
        if self.map_name is None:
            mapper = None
        else:
            mapper = getattr(program, self.map_name)

        for inkey, invalue in input:
            for key, value in mapper(inkey, invalue):
                yield (key, value)

    def to_args(self):
        return (self.op_name, self.map_name, self.part_name)


class ReduceOperation(Operation):
    op_name = 'reduce'
    task_class = ReduceTask

    def __init__(self, reduce_name, *args):
        Operation.__init__(self, *args)
        self.reduce_name = reduce_name
        self.id = '%s' % self.reduce_name

    def reduce(self, program, input):
        """Yields reduce output iterating over the entries in input.

        A reducer is an iterator taking a key and an iterator over values for
        that key.  It yields values for that key.
        """
        if self.reduce_name is None:
            reducer = None
        else:
            reducer = getattr(program, self.reduce_name)

        for key, iterator in grouped_read(input):
            for value in reducer(key, iterator):
                yield (key, value)

    def to_args(self):
        return (self.op_name, self.reduce_name, self.part_name)


class ReduceMapOperation(MapOperation, ReduceOperation):
    op_name = 'reducemap'
    task_class = ReduceMapTask

    def __init__(self, reduce_name, map_name, *args):
        Operation.__init__(self, *args)
        self.reduce_name = reduce_name
        self.map_name = map_name
        self.id = '%s_%s' % (self.reduce_name, self.map_name)

    def to_args(self):
        return (self.op_name, self.reduce_name, self.map_name, self.part_name)


def grouped_read(input_file):
    """Yields key-iterator pairs over a sorted input_file.

    This is very similar to itertools.groupby, except that we assume that
    the input_file is sorted, and we assume key-value pairs.
    """
    input_itr = iter(input_file)
    input = next(input_itr)
    next_pair = list(input)

    def subiterator():
        # Closure warning: don't rebind next_pair anywhere in this function
        group_key, value = next_pair

        while True:
            yield value
            try:
                input = next(input_itr)
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


OP_CLASSES = dict((op.op_name, op) for op in (MapOperation, ReduceOperation,
    ReduceMapOperation))

# vim: et sw=4 sts=4

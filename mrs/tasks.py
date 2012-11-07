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

"""MapReduce Tasks.

A Task represents a unit of work and the mechanism for carrying it out.
"""

from __future__ import division, print_function

import copy
import itertools
from operator import itemgetter

from . import datasets
from . import fileformats
from . import serializers
from . import util

from logging import getLogger
logger = getLogger('mrs')


class Task(object):
    """Manage input and output for a piece of a map or reduce operation.

    The `task_index` determines the split of the input dataset that will be
    used by this Task, as well as the source number that will be created by
    this Task.
    """
    def __init__(self, op, input_ds, dataset_id, task_index, splits, storage,
            ext, serializers):
        self.op = op
        self.input_ds = input_ds
        self.dataset_id = dataset_id
        self.task_index = task_index
        self.splits = splits
        self.storage = storage if (storage is not None) else ''
        self.ext = ext
        self.serializers = serializers

        self.outdir = None
        self.output = None
        self.sorted_ds = None

    def outurls(self):
        return [(b.split, b.url) for b in self.output[:, :] if b.url]

    @staticmethod
    def from_op(op, *args):
        return op.task_class(op, *args)

    @staticmethod
    def from_args(op_args, urls, dataset_id, task_index, splits, storage,
            ext, input_ser_names, ser_names, program):
        """Converts from a simple tuple to a Task.

        The elements of the tuple correspond to the arguments of the
        Task.__init__ method, with the difference that the first argument
        is an Operation args tuple, and the second is a list of urls.
        """
        op = Operation.from_args(*op_args)

        input_serializers = serializers.from_names(input_ser_names, program)
        output_serializers = serializers.from_names(ser_names, program)

        input_ds = datasets.FileData(urls, program, splits=1,
                first_split=task_index, serializers=input_serializers)
        return Task.from_op(op, input_ds, dataset_id, task_index, splits,
                storage, ext, output_serializers)

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

        input_serializers = self.input_ds.serializers

        if input_serializers:
            input_ser_names = (input_serializers.key_s_name,
                    input_serializers.value_s_name)
        else:
            input_ser_names = ''

        if self.serializers:
            ser_names = (self.serializers.key_s_name,
                    self.serializers.value_s_name)
        else:
            ser_names = ''

        return (op_args, urls, self.dataset_id, self.task_index, self.splits,
                self.storage, self.ext, input_ser_names, ser_names)

    def _get_all_input(self, serial, sort=False, default_dir=None,
            max_sort_size=None):
        """Returns an iterator over all input data."""
        if serial:
            self.input_ds.fetchall(_called_in_runner=True)
            uncopied_data = self.input_ds.data()
            # Avoid subtle race conditions in serial MapReduce when objects
            # are mutable by recklessly copying everything.  In the future, we
            # might make this disableable, but performance in serial MapReduce
            # is not a high priority.
            data = (copy.deepcopy(x) for x in uncopied_data)
            if sort:
                data = sorted(data, key=itemgetter(0))
        elif sort:
            tmpdir = util.mktempdir(default_dir, 'merge_%s_' % self.dataset_id)
            sorted_ds = datasets.MergeSortData(self.input_ds, self.task_index,
                    max_sort_size, dir=tmpdir, _called_in_runner=True)
            data = sorted_ds.stream_data(_called_in_runner=True)
            self.sorted_ds = sorted_ds
        else:
            data = self.input_ds.stream_split(self.task_index,
                    _called_in_runner=True)
        return data

    def _outdata_kwds(self, program, permanent, serial):
        """Returns arguments for the output dataset (common to all task types).
        """
        kwds = {'source': self.task_index,
                'parter': self.op.parter(program),
                'dir': self.outdir,
                'format': self.format(),
                'serializers': self.serializers,
                'splits': self.splits,
                }
        if not serial:
            kwds['write_only'] = True
        return kwds

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
        return format


class MapTask(Task):
    def run(self, program, default_dir, serial=False, max_sort_size=None):
        assert isinstance(self.op, MapOperation)

        all_input = self._get_all_input(serial)
        permanent = self.make_outdir(default_dir)
        kwds = self._outdata_kwds(program, permanent, serial)
        map_itr = self.op.map(program, all_input)
        self.output = datasets.LocalData(map_itr, permanent=permanent, **kwds)


class ReduceTask(Task):
    def run(self, program, default_dir, serial=False, max_sort_size=None):
        assert isinstance(self.op, ReduceOperation)

        all_input = self._get_all_input(serial, sort=True,
                default_dir=default_dir, max_sort_size=max_sort_size)

        permanent = self.make_outdir(default_dir)
        kwds = self._outdata_kwds(program, permanent, serial)
        reduce_itr = self.op.reduce(program, all_input)
        self.output = datasets.LocalData(reduce_itr, permanent=permanent,
                **kwds)
        if self.sorted_ds is not None:
            self.sorted_ds.delete()


class ReduceMapTask(Task):
    def run(self, program, default_dir, serial=False, max_sort_size=None):
        assert isinstance(self.op, ReduceMapOperation)

        all_input = self._get_all_input(serial, sort=True,
                default_dir=default_dir, max_sort_size=max_sort_size)

        permanent = self.make_outdir(default_dir)
        kwds = self._outdata_kwds(program, permanent, serial)
        reduce_itr = self.op.reduce(program, all_input)
        map_itr = self.op.map(program, reduce_itr)
        self.output = datasets.LocalData(map_itr, permanent=permanent, **kwds)
        if self.sorted_ds is not None:
            self.sorted_ds.delete()


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

    def __init__(self, map_name, combine_name, *args):
        Operation.__init__(self, *args)
        self.map_name = map_name
        self.combine_name = combine_name
        self.id = '%s' % self.map_name

    def map(self, program, input):
        """Yields map output iterating over the entries in input."""
        if self.map_name is None:
            mapper = None
        else:
            mapper = getattr(program, self.map_name)

        if self.combine_name:
            combine_op = ReduceOperation(self.combine_name, self.part_name)
        else:
            combine_op = None

        map_iter = self._map(mapper, input)
        if combine_op:
            # SORT PHASE
            sorted_map_iter = sorted(map_iter, key=itemgetter(0))
            combine_iter = combine_op.reduce(program, sorted_map_iter)
            return combine_iter
        else:
            return map_iter

    def _map(self, mapper, input):
        for inkey, invalue in input:
            for key, value in mapper(inkey, invalue):
                yield (key, value)

    def to_args(self):
        return (self.op_name, self.map_name, self.combine_name,
                self.part_name)


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

        grouped_input = ((k, (pair[1] for pair in v)) for k, v in
            itertools.groupby(input, key=itemgetter(0)))

        for key, iterator in grouped_input:
            for value in reducer(key, iterator):
                yield (key, value)

    def to_args(self):
        return (self.op_name, self.reduce_name, self.part_name)


class ReduceMapOperation(MapOperation, ReduceOperation):
    op_name = 'reducemap'
    task_class = ReduceMapTask

    def __init__(self, reduce_name, map_name, combine_name, *args):
        Operation.__init__(self, *args)
        self.reduce_name = reduce_name
        self.map_name = map_name
        self.combine_name = combine_name
        self.id = '%s_%s' % (self.reduce_name, self.map_name)

    def to_args(self):
        return (self.op_name, self.reduce_name, self.map_name,
                self.combine_name, self.part_name)


OP_CLASSES = dict((op.op_name, op) for op in (MapOperation, ReduceOperation,
    ReduceMapOperation))

# vim: et sw=4 sts=4

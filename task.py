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

"""MapReduce Tasks.

A Task represents a unit of work.  A Task is created on the master,
serialized, sent to the slave, and then created on the slave.
"""


class Task(object):
    """Manage input and output for a piece of a map or reduce operation.

    The `split` is the split of input that will be used by this Task.  Note
    that on the slave, this will always be 0 because the slave is unaware of
    other input that the master may know about.  The `source` is the source
    number that will be created by this Task.  It is just used for
    informational purposes, like naming output files.
    """
    def __init__(self, input, split, source, storage, format):
        self.input = input
        self.source = source
        self.split = split
        self.storage = storage
        self.dataset = None
        if format is None:
            from io.hexformat import HexWriter
            format = HexWriter
        self.format = format

        self.output = None
        self._outurls = []

    def active(self):
        self.dataset.task_started(self)

    def finished(self, urls=None):
        if urls:
            self._outurls = urls
        self.dataset.task_finished(self)

    def canceled(self):
        self.dataset.task_canceled(self)

    def inurls(self):
        buckets = self.input[:, self.split]

        urls = []
        for bucket in buckets:
            url = bucket.url
            if url is None:
                urls.append('')
            else:
                urls.append(url)
        return urls

    def outurls(self):
        # Normally, there's an output object, but the master only holds a
        # list of urls.
        if self.output:
            urls = []
            for bucket in self.output:
                if len(bucket):
                    urls.append(bucket.url)
                else:
                    urls.append('')
            return urls
        else:
            return self._outurls


class MapTask(Task):
    def __init__(self, input, split, source, mapper, parter, splits,
            storage, format):
        Task.__init__(self, input, split, source, storage, format)
        self.mapper = mapper
        self.partition = parter
        self.splits = splits

    def run(self, serial=False):
        import datasets
        from itertools import chain

        # SETUP INPUT
        self.input.fetchall(serial)
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
    def __init__(self, input, split, source, reducer, parter, splits,
            storage, format):
        Task.__init__(self, input, split, source, storage, format)
        self.reducer = reducer
        self.partition = parter
        self.splits = splits

    def run(self, serial=False):
        import datasets
        from itertools import chain

        # SORT PHASE
        self.input.fetchall(serial)
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

        This is very similar to itertools.groupby, except that we assume that the
        input_file is sorted, and we assume key-value pairs.
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



# vim: et sw=4 sts=4

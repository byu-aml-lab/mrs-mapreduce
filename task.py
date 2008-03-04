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

from partition import hash_partition

class Task(object):
    def __init__(self, taskid, input, outdir, format):
        self.taskid = taskid
        self.input = input
        self.outdir = outdir
        self.dataset = None
        self.format = format

        self.output = None
        self._outurls = []

        # TODO: check to see if there's somewhere better for this:
        from util import try_makedirs
        try_makedirs(outdir)

    def active(self):
        self.dataset.task_started(self)

    def finished(self, urls):
        self._outurls = urls
        self.dataset.task_finished(self)

    def canceled(self):
        self.dataset.task_canceled(self)

    def inurls(self):
        splits = self.input[:, self.taskid]

        urls = []
        for bucket in splits:
            url = bucket.url
            if url is None:
                urls.append('')
            else:
                urls.append(url)
        return urls

        # This one is bad because it produces Nones:
        #return [bucket.url for bucket in splits]

        # This one is better, but it takes more space and requires more
        # changes in other code:
        #return [(i, bucket.url) for i, bucket in enumerate(splits)
        #        if bucket.url is not None]

    def outurls(self):
        # Normally, there's an output object, but the master only holds a
        # list of urls.
        if self.output:
            return [bucket.url for bucket in self.output]
        else:
            return self._outurls

    def tempdir(self, prefix):
        import tempfile
        directory = tempfile.mkdtemp(dir=self.outdir,
                prefix=('%s_%s_' % (prefix, self.taskid)))
        return directory


class MapTask(Task):
    def __init__(self, taskid, input, map_name, part_name, nparts, outdir,
            format, registry):
        Task.__init__(self, taskid, input, outdir, format)
        self.map_name = map_name
        self.mapper = registry[map_name]
        self.part_name = part_name
        if part_name == '':
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self):
        import datasets
        from itertools import chain

        # PREP
        directory = self.tempdir('map')
        self.output = datasets.Output(self.partition, self.nparts,
                taskid=self.taskid, directory=directory)

        # SETUP INPUT
        self.input.fetchall()
        all_input = self.input.itersplit(0)

        # MAP PHASE
        self.output.collect(mrs_map(self.mapper, all_input))
        # TODO: this should happen automatically
        self.output.dump()


class ReduceTask(Task):
    def __init__(self, taskid, input, reduce_name, part_name, nparts, outdir,
            format, registry):
        Task.__init__(self, taskid, input, outdir, format)
        self.reduce_name = reduce_name
        self.reducer = registry[reduce_name]
        self.part_name = part_name
        if part_name == '':
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self):
        import datasets
        from itertools import chain

        # PREP
        directory = self.tempdir('reduce')
        self.output = datasets.Output(self.partition, self.nparts,
                directory=directory, format=self.format)

        # SORT PHASE
        # TODO: Set heap=True when there are still mappers running.  If all
        # mappers are finished, it just slows things down.
        #self.input.fetchall(heap=True)
        self.input.fetchall()
        all_input = sorted(self.input.itersplit(0))

        # Do the following if external sort is necessary (i.e., the input
        # files are too big to fit in memory):
        #fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        #os.close(fd)
        #io.hexfile_sort(interm_names, sorted_name)

        # REDUCE PHASE
        self.output.collect(mrs_reduce(self.reducer, all_input))
        # TODO: this should happen automatically
        self.output.dump()


def mrs_map(mapper, input):
    """Perform a map from the entries in input."""
    for inkey, invalue in input:
        for key, value in mapper(inkey, invalue):
            yield (key, value)

def grouped_read(input_file):
    """An iterator that yields key-iterator pairs over a sorted input_file.

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


def mrs_reduce(reducer, input):
    """Perform a reduce from the entries in input into output.

    A reducer is an iterator taking a key and an iterator over values for that
    key.  It yields values for that key.
    """
    for key, iterator in grouped_read(input):
        for value in reducer(key, iterator):
            yield (key, value)

# vim: et sw=4 sts=4

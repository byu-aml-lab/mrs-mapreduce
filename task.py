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


# TODO: source is really just used for naming output files; it should be
# replaced by a more descriptive parameter that makes it clear that it's
# not necessarily the source number.

class Task(object):
    """Manage input and output for a piece of a map or reduce operation.

    The `split` is the split of input that will be used by this Task.  Note
    that on the slave, this will always be 0 because the slave is unaware of
    other input that the master may know about.  The `source` is the source
    number that will be created by this Task.  It is just used for
    informational purposes, like naming output files.
    """
    def __init__(self, input, split, source, outdir, format):
        self.input = input
        self.source = source
        self.split = split
        self.outdir = outdir
        self.dataset = None
        self.format = format

        self.output = None
        self._outurls = []

        # TODO: check to see if there's somewhere better for this:
        if outdir:
            from util import try_makedirs
            try_makedirs(outdir)

    def active(self):
        self.dataset.task_started(self)

    def finished(self, urls=None):
        if urls:
            self._outurls = urls
        self.dataset.task_finished(self)

    def canceled(self):
        self.dataset.task_canceled(self)

    def inurls(self):
        splits = self.input[:, self.split]

        urls = []
        for bucket in splits:
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

    def tempdir(self, prefix):
        import tempfile
        directory = tempfile.mkdtemp(dir=self.outdir,
                prefix=('%s_%s_' % (prefix, self.source)))
        return directory


class MapTask(Task):
    def __init__(self, input, split, source, map_name, part_name, nparts,
            outdir, format, registry):
        Task.__init__(self, input, split, source, outdir, format)
        self.map_name = map_name
        self.mapper = registry[map_name]
        self.part_name = part_name
        if part_name == '':
            from partition import hash_partition
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self, serial=False):
        import datasets
        from itertools import chain

        # PREP
        if serial:
            directory = None
        else:
            directory = self.tempdir('map')
        self.output = datasets.Output(self.partition, self.nparts,
                source=self.source, directory=directory)

        # SETUP INPUT
        self.input.fetchall(serial)
        if serial:
            all_input = self.input.iterdata()
        else:
            all_input = self.input.itersplit(self.split)

        # MAP PHASE
        self.output.collect(self.map(all_input))

    def map(self, input):
        """Yields map output iterating over the entries in input."""
        for inkey, invalue in input:
            for key, value in self.mapper(inkey, invalue):
                yield (key, value)


class ReduceTask(Task):
    def __init__(self, input, split, source, reduce_name, part_name, nparts,
            outdir, format, registry):
        Task.__init__(self, input, split, source, outdir, format)
        self.reduce_name = reduce_name
        self.reducer = registry[reduce_name]
        self.part_name = part_name
        if part_name == '':
            from partition import hash_partition
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self, serial=False):
        import datasets
        from itertools import chain

        # PREP
        if serial:
            directory = None
        else:
            directory = self.tempdir('reduce')
        self.output = datasets.Output(self.partition, self.nparts,
                directory=directory, format=self.format)

        # SORT PHASE
        # TODO: Set heap=True when there are still mappers running.  If all
        # mappers are finished, it just slows things down.
        #self.input.fetchall(heap=True)
        self.input.fetchall(serial)
        if serial:
            all_input = self.input.iterdata()
        else:
            all_input = self.input.itersplit(self.split)
        sorted_input = sorted(all_input)

        # Do the following if external sort is necessary (i.e., the input
        # files are too big to fit in memory):
        #fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        #os.close(fd)
        #io.hexfile_sort(interm_names, sorted_name)

        # REDUCE PHASE
        self.output.collect(self.reduce(sorted_input))

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

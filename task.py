# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
# more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Mrs.  If not, see <http://www.gnu.org/licenses/>.


# TODO: taskid is really just used for naming output files; it should be
# replaced by a more descriptive parameter that makes it clear that it's
# not necessarily the taskid number.

class Task(object):
    def __init__(self, taskid, input, split, outdir, format):
        self.taskid = taskid
        self.input = input
        self.split = split
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
                prefix=('%s_%s_' % (prefix, self.taskid)))
        return directory


class MapTask(Task):
    def __init__(self, taskid, input, split, map_name, part_name, nparts,
            outdir, format, registry):
        Task.__init__(self, taskid, input, split, outdir, format)
        self.map_name = map_name
        self.mapper = registry[map_name]
        self.part_name = part_name
        if part_name == '':
            from partition import hash_partition
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self):
        import datasets
        from itertools import chain
        from mapreduce import mrs_map

        # PREP
        directory = self.tempdir('map')
        self.output = datasets.Output(self.partition, self.nparts,
                taskid=self.taskid, directory=directory)

        # SETUP INPUT
        self.input.fetchall()
        all_input = self.input.itersplit(self.split)

        # MAP PHASE
        self.output.collect(mrs_map(self.mapper, all_input))
        # TODO: this should happen automatically
        self.output.dump()


class ReduceTask(Task):
    def __init__(self, taskid, input, split, reduce_name, part_name, nparts,
            outdir, format, registry):
        Task.__init__(self, taskid, input, split, outdir, format)
        self.reduce_name = reduce_name
        self.reducer = registry[reduce_name]
        self.part_name = part_name
        if part_name == '':
            from partition import hash_partition
            self.partition = hash_partition
        else:
            self.partition = registry[part_name]
        self.nparts = nparts

    def run(self):
        import datasets
        from itertools import chain
        from mapreduce import mrs_reduce

        # PREP
        directory = self.tempdir('reduce')
        self.output = datasets.Output(self.partition, self.nparts,
                directory=directory, format=self.format)

        # SORT PHASE
        # TODO: Set heap=True when there are still mappers running.  If all
        # mappers are finished, it just slows things down.
        #self.input.fetchall(heap=True)
        self.input.fetchall()
        all_input = sorted(self.input.itersplit(self.split))

        # Do the following if external sort is necessary (i.e., the input
        # files are too big to fit in memory):
        #fd, sorted_name = tempfile.mkstemp(prefix='mrs.sorted_')
        #os.close(fd)
        #io.hexfile_sort(interm_names, sorted_name)

        # REDUCE PHASE
        self.output.collect(mrs_reduce(self.reducer, all_input))
        # TODO: this should happen automatically
        self.output.dump()


# vim: et sw=4 sts=4

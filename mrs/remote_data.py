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


# TODO: add a Dataset for resplitting input (right now we assume that input
# files are pre-split).

import os

from . import bucket
from . import datasets
from . import fileformats
from .tasks import Task


class RemoteData(datasets.BaseDataset):
    """A Dataset whose contents can be downloaded and read.

    Subclasses need to set the url for each bucket.
    """
    def __init__(self, **kwds):
        super(RemoteData, self).__init__(**kwds)

        self._urls_known = False
        self._fetched = False

    def _make_bucket(self, source, split):
        return bucket.ReadBucket(source, split)

    def __getstate__(self):
        """Pickle without getting certain forbidden/unnecessary elements."""
        state = self.__dict__.copy()
        del state['_close_callback']
        del state['_fetched']
        if self.closed:
            state['_data'] = None
        return state

    def __setstate__(self, dict):
        self.__dict__ = dict
        self._close_callback = None
        self._fetched = False

    # TODO: consider parallelizing this to use multiple downloading threads.
    def fetchall(self):
        """Download all of the files."""
        assert not self.closed, (
                'Invalid fetchall on a closed dataset.')

        # Don't call fetchall twice:
        if self._fetched:
            return

        assert self._urls_known, (
                'Invalid fetchall on a dataset with unknown urls.')

        for bucket in self[:, :]:
            url = bucket.url
            if url:
                reader = fileformats.open_url(url)
                bucket.collect(reader)
                reader.finish()

        self._fetched = True

    def notify_urls_known(self):
        """Signify that all buckets have been assigned urls."""
        self._urls_known = True


class FileData(RemoteData):
    """A list of static files or urls to be used as input to an operation.

    By default, all of the files come from a single source, with one split for
    each file.  If a split is given, then the dataset will have enough sources
    to evenly divide the files.

    >>> urls = ['http://aml.cs.byu.edu/', __file__]
    >>> data = FileData(urls)
    >>> len(data)
    2
    >>> data.fetchall()
    >>> data[0, 0][0]
    (0, '<html>\\n')
    >>> data[0, 0][1]
    (1, '<head>\\n')
    >>> data[0, 1][0]
    (0, '# Mrs\\n')
    >>>
    """
    def __init__(self, urls, splits=None, first_source=0, first_split=0,
            **kwds):
        n = len(urls)

        if splits is None:
            if sources is None:
                # Nothing specified, so we assume one split per url
                splits = n
            else:
                splits = first_split + n // sources

        super(FileData, self).__init__(splits=splits, **kwds)
        for i, url in enumerate(urls):
            if url:
                source = first_source + i // splits
                split = first_split + i % splits
                bucket = self[source, split]
                bucket.url = url
        self._urls_known = True


class ComputedData(RemoteData):
    """Manage input to or output from a map or reduce operation.

    The data are evaluated lazily.  A Dataset knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.

    Attributes:
        task_class: the class used to carry out computation
        parter: name of the partition function (see registry for more info)
    """
    def __init__(self, operation, input, **kwds):
        # Create exactly one task for each split in the input.
        self.ntasks = input.splits
        super(ComputedData, self).__init__(**kwds)

        self.op = operation
        self.id = '%s_%s' % (operation.id, self.id)

        self._computing = True

        assert(not input.closed)
        self.input_id = input.id

    def computation_done(self):
        """Signify that computation of the dataset is done."""
        self._computing = False

    def run_serial(self, program, datasets):
        input_data = datasets[self.input_id]
        self.splits = 1
        task = Task.from_op(self.op, input_data, 0, self.splits, self.dir,
                self.format, self.permanent)

        task.run(program, serial=True)
        self._use_output(task.output)
        task.output.close()
        self.computation_done()

    def get_task(self, task_index, datasets, jobdir):
        """Creates a task for the given source id.

        The program and datasets parameters are required for finding the
        function and inputs.  The jobdir parameter is used to create an output
        directory if one was not explicitly specified.
        """
        input_data = datasets[self.input_id]
        if jobdir and not self.dir:
            self.dir = os.path.join(jobdir, self.id)
            os.mkdir(self.dir)
        return Task.from_op(self.op, input_data, task_index, self.ntasks,
                self.dir, self.format, self.permanent)

    def fetchall(self):
        assert not self.computing, (
                'Invalid attempt to call fetchall on a non-ready dataset.')
        super(ComputedData, self).fetchall()

    def _use_output(self, output):
        """Uses the contents of the given LocalData."""
        self._data = output._data
        self.splits = len(output._data)
        self._fetched = True

    @property
    def computing(self):
        return self._computing


def test():
    import doctest
    doctest.testmod()


# vim: et sw=4 sts=4

# Mrs
# Copyright 2008-2012 Brigham Young University
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

from . import datasets
from .tasks import Task


class ComputedData(datasets.RemoteData):
    """Manage input to or output from a map or reduce operation.

    The data are evaluated lazily.  A Dataset knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.

    Arguments:
        affinity: whether to try to assign tasks to the same slave that
            computed the input task of the same id (often true for iterative
            programs)
        async_start: whether to allow the dataset to start asynchronously
            (while some tasks in the parent are still running)
        blocking_percent: dependent datasets may start being computed when
            the given percent of tasks are completed
        backlink: any uncompleted tasks from the given dataset will be
            "pulled forward" into place in the current dataset

    Attributes:
        task_class: the class used to carry out computation
        parter: name of the partition function (see registry for more info)
        backlink_id: string id of the dataset backlinked to
    """
    def __init__(self, operation, input, splits, affinity=False,
            blocking_percent=1, backlink=None, async_start=False, **kwds):
        # Create exactly one task for each split in the input.
        self.ntasks = input.splits

        super(ComputedData, self).__init__(**kwds)

        self.op = operation
        self.splits = splits
        self.id = '%s_%s' % (operation.id, self.id)

        self._computing = True

        assert not input.closed
        self.input_id = input.id

        # Options
        self.affinity = affinity
        self.blocking_percent = blocking_percent
        self.async_start = async_start
        if backlink is None:
            self.backlink_id = None
        elif not isinstance(backlink, ComputedData):
            raise RuntimeError('Only ComputedDatas can be backlinked to')
        elif backlink.closed:
            raise RuntimeError('A closed dataset cannot be backlinked to')
        else:
            self.backlink_id = backlink.id

    def computation_done(self):
        """Signify that computation of the dataset is done."""
        self._computing = False

    def run_serial(self, program, datasets):
        input_data = datasets[self.input_id]
        self.splits = 1
        if self.format is not None:
            ext = self.format.ext
        else:
            ext = ''
        task = Task.from_op(self.op, input_data, self.id, 0, self.splits,
                self.dir, ext, self.serializers)

        task.run(program, None, serial=True)
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
        if self.format is not None:
            ext = self.format.ext
        else:
            ext = ''
        return Task.from_op(self.op, input_data, self.id, task_index,
                self.splits, self.dir, ext, self.serializers)

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

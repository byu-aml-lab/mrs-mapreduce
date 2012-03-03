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

from . import datasets
from .tasks import Task


class ComputedData(datasets.RemoteData):
    """Manage input to or output from a map or reduce operation.

    The data are evaluated lazily.  A Dataset knows how to generate or
    regenerate its contents.  It can also decide whether to save the data to
    permanent storage or to leave them in memory on the slaves.

    Attributes:
        task_class: the class used to carry out computation
        parter: name of the partition function (see registry for more info)
    """
    def __init__(self, operation, input, splits, **kwds):
        # Create exactly one task for each split in the input.
        self.ntasks = input.splits
        super(ComputedData, self).__init__(**kwds)

        self.op = operation
        self.splits = splits
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
        if self.format is not None:
            ext = self.format.ext
        else:
            ext = ''
        task = Task.from_op(self.op, input_data, self.id, 0, self.splits,
                self.dir, ext)

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
                self.splits, self.dir, ext)

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

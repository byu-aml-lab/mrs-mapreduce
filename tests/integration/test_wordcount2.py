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

from __future__ import print_function

from collections import defaultdict
import glob
import tempfile

from .conftest import run_serial, run_mockparallel, run_master_slave
from .wordcount2 import WordCount2


def test_dickens(mrs_impl, mrs_reduce_tasks, tmpdir):
    inputs = glob.glob('tests/data/dickens/*')
    outdir = tmpdir.join('out')
    with tempfile.NamedTemporaryFile('wt') as listfile:
        for filename in inputs:
            print(filename, file=listfile)
        listfile.flush()

        args = [listfile.name, outdir.strpath]

        if mrs_impl == 'serial':
            assert mrs_reduce_tasks == 1
            run_serial(WordCount2, args)
        elif mrs_impl == 'mockparallel':
            args = ['--mrs-reduce-tasks', str(mrs_reduce_tasks)] + args
            run_mockparallel(WordCount2, args, tmpdir)
        elif mrs_impl == 'master_slave':
            args = ['--mrs-reduce-tasks', str(mrs_reduce_tasks)] + args
            run_master_slave(WordCount2, args, tmpdir)
        else:
            raise RuntimeError('Unknown mrs_impl: %s' % mrs_impl)

    files = outdir.listdir()
    assert len(files) == mrs_reduce_tasks

    counts = defaultdict(int)
    for outfile in files:
        text = outfile.readlines()
        last_key = None
        for line in text:
            key, value = line.split()
            counts[key] += int(value)
            assert last_key is None or last_key <= key
            last_key = key

    # Check counts for all of the words in the first two lines.
    assert counts['it'] == 11
    assert counts['was'] == 12
    assert counts['the'] == 18
    assert counts['best'] == 1
    assert counts['of'] == 16
    assert counts['worst'] == 1
    assert counts['times'] == 2

    # Check counts for several other miscellaneous words.
    assert counts['a'] == 8
    assert counts['heaven'] == 1
    assert counts['period'] == 2
    assert counts['settled'] == 1
    assert counts['for'] == 3

# vim: et sw=4 sts=4

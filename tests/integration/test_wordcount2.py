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
        for line in text:
            key, value = line.split()
            counts[key] += int(value)

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

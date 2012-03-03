from collections import defaultdict
import glob

from wordcount import WordCount


def test_dickens(mrs_impl, mrs_reduce_tasks, tmpdir):
    inputs = glob.glob('tests/data/dickens/*')
    args = inputs + [tmpdir.strpath]

    mrs_impl(WordCount, args, mrs_reduce_tasks)

    files = tmpdir.listdir()
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

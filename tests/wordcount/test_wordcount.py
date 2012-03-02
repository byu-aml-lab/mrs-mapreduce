import glob
import mrs
import os
import pytest

from wordcount import WordCount


def test_serial(tmpdir):
    inputs = glob.glob('tests/data/dickens/*')
    args = ['-I', 'Serial'] + inputs + [tmpdir.strpath]

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(WordCount, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0

    files = tmpdir.listdir()
    assert len(files) == 1
    outfile = files[0]
    text = outfile.readlines()

    assert text[0] == 'a 8\n'
    counts = {}
    for line in text:
        key, value = line.split()
        counts[key] = int(value)

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

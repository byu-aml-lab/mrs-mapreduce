import glob
from mrs.fileformats import LineReader
import sys

try:
    from cStringIO import StringIO as BytesIO
except ImportError:
    from io import BytesIO

PY3 = sys.version_info[0] == 3


def test_dickens():
    with open('tests/data/dickens/split1.txt', 'rb') as f:
        reader = LineReader(f)
        lines = list(reader)
        assert len(lines) == 2

        key, value = lines[0]
        assert key == 0
        assert value == 'It was the best of times,\n'

        key, value = lines[1]
        assert key == 1
        assert value == 'it was the worst of times,\n'

def test_bytesio():
    orig_lines = ['line 1\n', 'line 2\n', '3\tline\n']
    if PY3:
        orig_lines.append('line ∞\n')
    else:
        orig_lines = [unicode(s) for s in orig_lines]
        orig_lines.append('line \xe2\x88\x9e\n'.decode('utf-8'))
    data = ''.join(orig_lines).encode('utf-8')
    f = BytesIO(data)

    reader = LineReader(f)
    lines = list(reader)

    assert lines == list(enumerate(orig_lines))

# vim: et sw=4 sts=4

from mrs.bucket import WriteBucket
from mrs import BinWriter, HexWriter

def test_writebucket():
    b = WriteBucket(0, 0)
    b.addpair((4, 'test'))
    b.collect([(3, 'a'), (1, 'This'), (2, 'is')])

    values = ' '.join(value for key, value in b)
    assert values == 'test a This is'

    b.sort()
    values = ' '.join(value for key, value in b)
    assert values == 'This is a test'

def test_write_only():
    b = WriteBucket(0, 0)
    b.addpair((4, 'test'), write_only=True)
    b.collect([(3, 'a'), (1, 'This'), (2, 'is')], write_only=True)

    values = ' '.join(value for key, value in b)
    assert values == ''

    readonly_copy = b.readonly_copy()
    assert readonly_copy.url is None

def test_writing(tmpdir):
    b = WriteBucket(2, 4, dir=tmpdir.strpath, format=BinWriter)
    prefix = b.prefix()
    assert prefix == 'source_2_split_4_'

    listdir = tmpdir.listdir()
    assert listdir == []

    b.addpair((1, 2))

    filename = prefix + '.mrsb'
    path = tmpdir.join(filename).strpath
    listdir = tmpdir.listdir()
    assert listdir == [path]

    readonly_copy = b.readonly_copy()
    assert readonly_copy.url == path

def test_roundtrip(tmpdir):
    b = WriteBucket(2, 4, dir=tmpdir.strpath, format=BinWriter)
    prefix = b.prefix()
    assert prefix == 'source_2_split_4_'

    listdir = tmpdir.listdir()
    assert listdir == []

    b.addpair((4, 'test'))
    b.collect([(3, 'a'), (1, 'This'), (2, 'is')])

    values = ' '.join(value for key, value in b)
    assert values == 'test a This is'

    b.close_writer(do_sync=False)

    filename = prefix + '.mrsb'
    path = tmpdir.join(filename).strpath
    listdir = tmpdir.listdir()
    assert listdir == [path]

    readonly_copy = b.readonly_copy()
    assert readonly_copy.url == path
    values = ' '.join(value for key, value in readonly_copy)
    assert values == 'test a This is'

    values = ' '.join(value for key, value in readonly_copy.stream())
    assert values == 'test a This is'

    b.clean()
    listdir = tmpdir.listdir()
    assert listdir == []

def test_roundtrip_write_only(tmpdir):
    b = WriteBucket(7, 1, dir=tmpdir.strpath, format=HexWriter)
    prefix = b.prefix()
    assert prefix == 'source_7_split_1_'

    listdir = tmpdir.listdir()
    assert listdir == []

    b.addpair((4, 'test'), write_only=True)
    b.collect([(3, 'a'), (1, 'This'), (2, 'is')], write_only=True)

    values = ' '.join(value for key, value in b)
    assert values == ''

    b.close_writer(do_sync=False)

    filename = prefix + '.mrsx'
    path = tmpdir.join(filename).strpath
    listdir = tmpdir.listdir()
    assert listdir == [path]

    readonly_copy = b.readonly_copy()
    assert readonly_copy.url == path
    values = ' '.join(value for key, value in readonly_copy)
    assert values == ''

    values = ' '.join(value for key, value in readonly_copy.stream())
    assert values == 'test a This is'

    b.clean()
    listdir = tmpdir.listdir()
    assert listdir == []

# vim: et sw=4 sts=4

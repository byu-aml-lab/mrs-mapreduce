import pytest
from mrs import hdfs
from mrs import util


@pytest.mark.hadoop
def test_home_directory():
    homedir = hdfs.hdfs_get_home_directory('0potato', 'amcnabb')
    assert homedir == '/user/amcnabb'

@pytest.mark.hadoop
def test_round_trip():
    path = '/tmp/hello-%s.txt' % util.random_string(6)
    contents = 'Hello, world!\n'

    hdfs.hdfs_create('0potato', 'amcnabb', path, contents)

    data = hdfs.hdfs_open('0potato', 'amcnabb', path).read()
    assert data == contents

    result = hdfs.hdfs_delete('0potato', 'amcnabb', path)
    assert result == True

@pytest.mark.hadoop
def test_missing_file():
    path = '/aeuaoeu/oeau/aoeu/oaeuoaeu/aoeuaoeu'
    with pytest.raises(hdfs.FileNotFoundException):
        hdfs.hdfs_open('0potato', 'amcnabb', path)


if __name__ == '__main__':
    test_hdfs()

# vim: et sw=4 sts=4

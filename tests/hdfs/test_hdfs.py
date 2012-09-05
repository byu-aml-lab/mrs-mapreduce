import pytest
from mrs.hdfs import WebHDFSProxy
from mrs import util


@pytest.mark.hadoop
def test_home_directory():
    server = WebHDFSProxy('0potato', 'amcnabb')
    assert server.get_home_directory() == '/user/amcnabb'

@pytest.mark.hadoop
def test_round_trip():
    path = '/tmp/hello-%s.txt' % util.random_string(6)
    contents = 'Hello, world!\n'

    server = WebHDFSProxy('0potato', 'amcnabb')
    server.create(path, contents)

    data = server.open(path)
    assert data == contents

    result = server.delete(path)
    assert result == True


if __name__ == '__main__':
    test_hdfs()

# vim: et sw=4 sts=4

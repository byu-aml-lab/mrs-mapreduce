import pytest
from mrs.hdfs import WebHDFSProxy


@pytest.mark.hadoop
def test_home_directory():
    server = WebHDFSProxy('0potato', 'amcnabb')
    assert server.get_home_directory() == '/user/amcnabb'

@pytest.mark.hadoop
def test_round_trip():
    path = '/tmp/hello.txt'
    contents = 'Hello, world!\n'
    server = WebHDFSProxy('0potato', 'amcnabb')
    server.create(path, contents)
    data = server.open(path)
    assert data == contents


if __name__ == '__main__':
    test_hdfs()

# vim: et sw=4 sts=4

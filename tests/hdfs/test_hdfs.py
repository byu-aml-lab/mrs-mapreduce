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

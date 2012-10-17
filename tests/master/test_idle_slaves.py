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
from mrs.master import IdleSlaves

class Slave(object):
    def __init__(self, host, slave_id):
        self.host = host
        self.id = slave_id

    def __str__(self):
        return '%s-%s' % (self.host, self.id)

def test_one_host():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    # Create a new slaves list.
    slaves = IdleSlaves()

    # Add some slaves.
    slaves.add(slave1)
    assert slaves._max_count == 1
    slaves.add(slave2)
    assert slaves._max_count == 2

    # Remove some slaves.
    slaves.remove(slave1)
    assert slaves._max_count == 1
    with pytest.raises(KeyError):
        slaves.remove(slave1)
    assert slaves._max_count == 1
    slaves._consistency_check()
    slaves.remove(slave2)
    assert slaves._max_count == 0
    slaves._consistency_check()

    with pytest.raises(KeyError):
        slaves.pop()

def test_nonzero():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    # Create a new slaves list.
    slaves = IdleSlaves()

    # test __nonzero__
    assert bool(slaves) == False
    assert len(slaves) == 0
    slaves.add(slave1)
    assert bool(slaves) == True
    assert len(slaves) == 1

def test_contains():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    # Create a new slaves list.
    slaves = IdleSlaves()

    slaves.add(slave1)
    assert slave1 in slaves
    assert slave2 not in slaves
    assert len(slaves) == 1
    slaves._consistency_check()

def test_add_twice():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    # Create a new slaves list.
    slaves = IdleSlaves()
    assert len(slaves) == 0
    slaves._consistency_check()

    # Add some slaves.
    slaves.add(slave1)
    assert slaves._max_count == 1
    assert len(slaves) == 1
    slaves._consistency_check()
    slaves.add(slave2)
    assert slaves._max_count == 2
    assert len(slaves) == 2
    slaves._consistency_check()
    # Add the same slave a second time.
    slaves.add(slave2)
    assert slaves._max_count == 2
    assert len(slaves) == 2
    slaves._consistency_check()

def test_two_hosts():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    host2 = 'host2'
    slave3 = Slave(host2, 'slave3')
    slave4 = Slave(host2, 'slave4')
    slave5 = Slave(host2, 'slave5')

    # Create a new slaves list.
    slaves = IdleSlaves()

    # Add some slaves.
    slaves.add(slave1)
    assert slaves._max_count == 1
    slaves._consistency_check()
    slaves.add(slave2)
    assert slaves._max_count == 2
    slaves._consistency_check()
    slaves.add(slave3)
    assert slaves._max_count == 2
    slaves._consistency_check()
    slaves.add(slave4)
    assert slaves._max_count == 2
    slaves._consistency_check()
    slaves.add(slave5)
    assert slaves._max_count == 3
    slaves._consistency_check()

    # Pop a slave.
    popped_slave = slaves.pop()
    assert popped_slave.host == host2
    assert slaves._max_count == 2
    slaves._consistency_check()

    # Make sure that additional slaves are popped for alternating hosts.
    popped2 = slaves.pop()
    assert slaves._max_count == 2
    slaves._consistency_check()
    popped3 = slaves.pop()
    assert slaves._max_count == 1
    assert popped2.host != popped3.host
    slaves._consistency_check()

def test_add_to_smaller_host():
    host1 = 'host1'
    slave1 = Slave(host1, 'slave1')
    slave2 = Slave(host1, 'slave2')

    host2 = 'host2'
    slave3 = Slave(host2, 'slave3')
    slave4 = Slave(host2, 'slave4')
    slave5 = Slave(host2, 'slave5')

    # Create a new slaves list.
    slaves = IdleSlaves()
    assert len(slaves) == 0

    # Add some slaves.
    slaves.add(slave1)
    assert slaves._max_count == 1
    slaves._consistency_check()
    slaves.add(slave3)
    assert slaves._max_count == 1
    slaves._consistency_check()
    slaves.add(slave4)
    assert slaves._max_count == 2
    slaves._consistency_check()
    slaves.add(slave5)
    assert slaves._max_count == 3
    slaves._consistency_check()
    # Add a slave to the smaller host.
    slaves.add(slave2)
    assert slaves._max_count == 3
    slaves._consistency_check()


# vim: et sw=4 sts=4

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

from __future__ import division, print_function

from multiprocessing import Process
import pytest
import time


def pytest_generate_tests(metafunc):
    if 'mrs_impl' in metafunc.funcargnames:
        if 'mrs_reduce_tasks' in metafunc.funcargnames:
            metafunc.addcall(funcargs={'mrs_impl': 'serial',
                'mrs_reduce_tasks': 1})
            for i in (1, 3, 5):
                metafunc.addcall(funcargs={'mrs_impl': 'mockparallel',
                    'mrs_reduce_tasks': i})
            metafunc.addcall(funcargs={'mrs_impl': 'master_slave',
                'mrs_reduce_tasks': 1})
        else:
            for mrs_impl in ['serial', 'mockparallel', 'master_slave']:
                metafunc.addcall(funcargs={'mrs_impl': mrs_impl})


# vim: et sw=4 sts=4

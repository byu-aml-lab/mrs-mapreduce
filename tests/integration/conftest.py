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

import mrs
from multiprocessing import Process
import pytest
import time


def pytest_generate_tests(metafunc):
    if 'mrs_impl' in metafunc.funcargnames:
        #metafunc.parametrize('mrs_impl', [run_mrs.serial, run_mrs.mockparallel])
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


def run_serial(program, args):
    args = ['-I', 'Serial'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0


def run_mockparallel(program, args, tmpdir):
    args = ['-I', 'MockParallel', '--mrs-tmpdir', tmpdir.strpath] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0


def run_master_slave(program, args, tmpdir):
    runfile = tmpdir.join('runfile')

    procs = []
    for i in range(2):
        p = Process(target=slave_process, args=(program, runfile, tmpdir))
        p.start()
        procs.append(p)

    args = ['-I', 'Master', '--mrs-runfile', runfile.strpath, '--mrs-tmpdir',
            tmpdir.strpath] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0

    for p in procs:
        p.join()
        assert p.exitcode == 0

    runfile.remove()


def run_slave(program, master, tmpdir):
    args = ['-I', 'Slave', '--mrs-master', master, '--mrs-tmpdir',
            tmpdir.strpath]

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    if not excinfo.value.args:
        assert False, 'SystemExit raised without a value.'
    exitcode = excinfo.value.args[0]
    assert exitcode == 0


def slave_process(program, runfile, tmpdir):
    start = time.time()
    first = True
    while True:
        assert time.time() - start < 5, 'Master failed to start promptly.'
        try:
            with open(runfile.strpath) as f:
                port = f.read().strip()
                if port == '-':
                    # The master has already finished.
                    return
                elif port:
                    # A port is written to the file.
                    break
                # Otherwise, the file hasn't been written yet.
        except IOError:
            pass
        time.sleep(0.05)

    master = '127.0.0.1:%s' % port
    run_slave(program, master, tmpdir)

# vim: et sw=4 sts=4

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


def run_serial(program, args, update_parser=None):
    args = ['-I', 'Serial'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, update_parser, args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0


def run_mockparallel(program, args, tmpdir, update_parser=None):
    args = ['-I', 'MockParallel', '--mrs-tmpdir', tmpdir.strpath] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, update_parser, args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0


def run_master_slave(program, args, tmpdir, update_parser=None):
    runfile = tmpdir.join('runfile').strpath

    procs = []
    for i in range(2):
        p = Process(target=slave_process, args=(program, runfile, tmpdir))
        p.start()
        procs.append(p)

    args = ['-I', 'Master', '--mrs-runfile', runfile, '--mrs-tmpdir',
            tmpdir.strpath] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, update_parser, args)
    exitcode = excinfo.value.args[0]
    assert exitcode == 0

    for p in procs:
        p.join()
        assert p.exitcode == 0


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
    while True:
        try:
            with open(runfile) as f:
                port = f.read().strip()
                break
        except IOError:
            time.sleep(1)

    master = '127.0.0.1:%s' % port
    run_slave(program, master, tmpdir)

# vim: et sw=4 sts=4

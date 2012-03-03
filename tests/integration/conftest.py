import mrs
from multiprocessing import Process
import pytest
import time


def pytest_generate_tests(metafunc):
    if 'mrs_impl' in metafunc.funcargnames:
        #metafunc.parametrize('mrs_impl', [run_mrs.serial, run_mrs.mockparallel])
        if 'mrs_reduce_tasks' in metafunc.funcargnames:
            metafunc.addcall(funcargs={'mrs_impl': run_serial,
                'mrs_reduce_tasks': 1})
            for i in (1, 3, 5):
                metafunc.addcall(funcargs={'mrs_impl': run_mockparallel,
                    'mrs_reduce_tasks': i})
            metafunc.addcall(funcargs={'mrs_impl': run_master_slave,
                'mrs_reduce_tasks': 1})
        else:
            for mrs_impl in [run_serial, run_mockparallel]:
                metafunc.addcall(funcargs={'mrs_impl': mrs_impl})


def run_serial(program, args, mrs_reduce_tasks, tmpdir):
    assert mrs_reduce_tasks == 1
    args = ['-I', 'Serial'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


def run_mockparallel(program, args, mrs_reduce_tasks, tmpdir):
    args = (['-I', 'MockParallel', '--mrs-reduce-tasks', str(mrs_reduce_tasks)]
            + args)

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


def run_master_slave(program, args, mrs_reduce_tasks, tmpdir):
    runfile = tmpdir.join('runfile').strpath

    procs = []
    for i in range(2):
        p = Process(target=slave_process, args=(program, runfile))
        p.start()
        procs.append(p)

    args = ['-I', 'Master', '--mrs-reduce-tasks', str(mrs_reduce_tasks),
            '--mrs-runfile', runfile] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0

    for p in procs:
        p.join()
        assert p.exitcode == 0


def run_slave(program, master):
    args = ['-I', 'Slave', '--mrs-master', master]

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


def slave_process(program, runfile):
    while True:
        try:
            with open(runfile) as f:
                port = f.read().strip()
                break
        except IOError:
            time.sleep(1)

    master = '127.0.0.1:%s' % port
    run_slave(program, master)

# vim: et sw=4 sts=4

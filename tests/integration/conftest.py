import mrs
import pytest


def pytest_generate_tests(metafunc):
    if 'mrs_impl' in metafunc.funcargnames:
        #metafunc.parametrize('mrs_impl', [run_mrs.serial, run_mrs.mockparallel])
        if 'mrs_reduce_tasks' in metafunc.funcargnames:
            metafunc.addcall(funcargs={'mrs_impl': run_serial,
                'mrs_reduce_tasks': 1})
            for i in (1, 3, 5):
                metafunc.addcall(funcargs={'mrs_impl': run_mockparallel,
                    'mrs_reduce_tasks': i})
        else:
            for mrs_impl in [run_serial, run_mockparallel]:
                metafunc.addcall(funcargs={'mrs_impl': mrs_impl})


def run_serial(program_class, args, mrs_reduce_tasks):
    assert mrs_reduce_tasks == 1
    args = ['-I', 'Serial'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program_class, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


def run_mockparallel(program_class, args, mrs_reduce_tasks):
    args = (['-I', 'MockParallel', '--mrs-reduce-tasks', str(mrs_reduce_tasks)]
            + args)

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program_class, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


# vim: et sw=4 sts=4

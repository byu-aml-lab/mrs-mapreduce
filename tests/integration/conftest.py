import mrs
import pytest


def pytest_generate_tests(metafunc):
    if 'impl' in metafunc.funcargnames:
        #metafunc.parametrize('impl', [run_mrs.serial, run_mrs.mockparallel])
        for impl in [run_serial, run_mockparallel]:
            metafunc.addcall(funcargs={'impl': impl})


def run_serial(program_class, args):
    args = ['-I', 'Serial'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program_class, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


def run_mockparallel(program_class, args):
    args = ['-I', 'MockParallel'] + args

    with pytest.raises(SystemExit) as excinfo:
        mrs.main(program_class, args=args)
    returncode = excinfo.value.args[0]
    assert returncode == 0


# vim: et sw=4 sts=4

import pytest

from mrs import param

from .conftest import ParseFailed


def test_basic(parser):
    argv = ['--obj', 'mrs.param._Rabbit', '--obj-weight', '17']
    opts, args = parser.parse_args(argv)
    obj = param.instantiate(opts, 'obj')

    assert isinstance(obj, param._Rabbit)
    assert obj.weight == 17


def test_hyphens_to_underscores(parser):
    argv = ['--obj', 'mrs.param._Rabbit', '--obj-a-b-c', 'hi']
    opts, args = parser.parse_args(argv)
    obj = param.instantiate(opts, 'obj')

    assert isinstance(obj, param._Rabbit)
    assert obj.a_b_c == 'hi'


def test_missing_import(parser):
    argv = ['--obj', 'zzzzz']
    with pytest.raises(ParseFailed) as excinfo:
        parser.parse_args(argv)
    e = excinfo.value
    expected_msg = 'option --obj: Could not find "zzzzz" in the search path'
    assert e.args[0] == expected_msg


# vim: et sw=4 sts=4

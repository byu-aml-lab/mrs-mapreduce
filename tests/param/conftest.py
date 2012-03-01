import types

from mrs import param


def pytest_funcarg__parser(request):
    parser = param.OptionParser()
    parser.error = types.MethodType(parse_error, parser)
    parser.add_option('--obj', action='extend', dest='obj')
    return parser


class ParseFailed(Exception):
    """Exception to be raised by an OptionParser when a doctest fails."""


def parse_error(self, msg):
    """Instead of printing to stderr and exiting, just raise TestFailed."""
    raise ParseFailed(msg)

# vim: et sw=4 sts=4

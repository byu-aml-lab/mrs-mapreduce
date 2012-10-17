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

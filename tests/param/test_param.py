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

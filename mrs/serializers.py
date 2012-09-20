# Mrs
# Copyright 2008-2012 Brigham Young University
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# Mrs.  If not, see <http://www.gnu.org/licenses/>.
#
# Inquiries regarding any further use of Mrs, please contact the Copyright
# Licensing Office, Brigham Young University, 3760 HBLL, Provo, UT 84602,
# (801) 422-9339 or 422-3821, e-mail copyright@byu.edu.

from __future__ import division, print_function

from collections import namedtuple


Serializer = namedtuple('Serializer', ('dumps', 'loads'))

def key_serializer(serializer):
    """A decorator to specify a serializer for map or reduce functions.

    The given `serializer` is a string that is mapped to a Serializer object
    by the `MapReduce.serializer` method.
    """
    def wrapper(f):
        f.key_serializer = serializer
        return f
    return wrapper

def value_serializer(serializer):
    """A decorator to specify a serializer for map or reduce functions.

    The given `serializer` is a string that is mapped to a Serializer object
    by the `MapReduce.serializer` method.
    """
    def wrapper(f):
        f.value_serializer = serializer
        return f
    return wrapper


class Serializers(object):
    """Keeps track of a pair of serializers and their names."""
    def __init__(self, key_s, key_s_name, value_s,
            value_s_name):
        self.key_s = key_s
        self.key_s_name = key_s_name
        self.value_s = value_s
        self.value_s_name = value_s_name

    def names(self):
        """Returns a pair of serializer names."""
        key_s_name = self.key_s_name if self.key_s_name else ''
        value_s_name = self.value_s_name if self.value_s_name else ''
        return key_s_name, value_s_name

    @classmethod
    def from_names(cls, names, program):
        """Creates a Serializers from a pair of names and a MapReduce program.
        """
        if not names:
            return None
        key_s_name, value_s_name = names
        if key_s_name and hasattr(program, 'serializer'):
            key_s = program.serializer(key_s_name)
        else:
            key_s = None
        if value_s_name and hasattr(program, 'serializer'):
            value_s = program.serializer(value_s_name)
        else:
            value_s = None
        return cls(key_s, key_s_name, value_s, value_s_name)


###############################################################################
# str <-> bytes

def str_loads(b):
    return b.decode('utf-8')

def str_dumps(s):
    return s.encode('utf-8')

str_serializer = Serializer(str_dumps, str_loads)

###############################################################################
# int <-> bytes

def int_loads(b):
    return int(b.decode('utf-8'))

def int_dumps(i):
    return str(i).encode('utf-8')

int_serializer = Serializer(int_dumps, int_loads)

# vim: et sw=4 sts=4

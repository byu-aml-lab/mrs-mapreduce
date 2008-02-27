# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
# more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Mrs.  If not, see <http://www.gnu.org/licenses/>.


def hash_partition(x, n):
    """A partition function that partitions by hashing the key.
    
    The hash partition function is useful if the keys are not contiguous and
    want to make sure that the partitions are sized as equally as possible.
    """
    return hash(x) % n

def mod_partition(x, n):
    """A partition function that partitions by modding the key.
    
    The mod partition function is useful if your keys are contiguous and you
    want to make sure that the partitions are sized equally.
    """
    return x % n


# vim: et sw=4 sts=4

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

# TODO: fix the sample code in the following docstring:
"""Mrs: MapReduce - a Simple implementation

Your Mrs MapReduce program might look something like this:

def mapper(key, value):
    yield newkey, newvalue

def reducer(key, values):
    yield newvalue

if __name__ == '__main__':
    import mrs
    mrs.main(mapper, reducer)
"""

# rather than importing all submodules, we just import the ones that are
# expected to be useful outside of Mrs internals.
import cli, datasets, partition, registry, version

VERSION = version.VERSION
Registry = registry.Registry
HexWriter = io.HexWriter
TextWriter = io.TextWriter
main = cli.main
primary_impl = cli.primary_impl
hash_partition = partition.hash_partition
mod_partition = partition.mod_partition

# vim: et sw=4 sts=4

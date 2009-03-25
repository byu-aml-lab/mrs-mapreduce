# Mrs
# Copyright 2008-2009 Brigham Young University
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

"""Standard MapReduce programs.

For now, this module consists of the MapReduce class, which provides the
implementation for a standard MapReduce program.  However, the MapReduce class
can be extended to create much more complex programs.
"""


class MapReduce(object):
    """MapReduce program definition.

    MapReduce must be extended to be useful.  The default run method creates a
    map stage and a reduce stage, using the map and reduce methods
    respectively.  These methods have no default implementation and must be
    extended.  The simplest usable program (see wordcount.py) simply provides
    implementations for map and reduce.

    A program may optionally override the `run`, `__init__`, and `bypass`
    methods.  The `run` method defines the stages of a MapReduce job.  Its
    default implementation consists of a map stage followed by a reduce stage.
    The `__init__` method gets called on the master and each worker.  It takes
    a single argument opts, which contains the values parsed from the
    command-line.  The `bypass` method allows a program to define an alternate
    implementation that bypasses the MapReduce framework.

    It is important to understand that a separate MapReduce instance is
    created on the master and each worker.  The map and reduce methods cannot
    safely write state to self, since this state will not be shared between
    workers.

    Attributes:
        opts: An optparse.Values instance with all command-line options.
        args: A list of command-line positional arguments.
    """
    def map(self, key, value):
        """Map function.

        A map function takes a key and a value and yields new (key, value)
        pairs.
        """
        raise NotImplementedError

    def reduce(self, key, values):
        """Reduce function.

        A reduce function takes a key and a value iterator and yields new
        values.
        """
        raise NotImplementedError

    def __init__(self, opts, args):
        self.opts = opts
        self.args = args

    def run(self, job):
        """Default run which creates a map stage and a reduce stage."""
        from io.textformat import TextWriter

        if len(self.args) < 2:
            import sys
            print >>sys.stderr, "Requires input(s) and an output."
            return
        inputs = self.args[:-1]
        outdir = self.args[-1]

        source = job.file_data(inputs)
        intermediate = job.map_data(source, self.map)
        output = job.reduce_data(intermediate, self.reduce, outdir=outdir,
                format=TextWriter)
        job.end()

        ready = []
        while not ready:
            ready = job.wait(output, timeout=2.0)
            print job.status()

    def hash_partition(self, x, n):
        """A partition function that partitions by hashing the key.

        The hash partition function is useful if the keys are not contiguous and
        want to make sure that the partitions are sized as equally as possible.
        """
        return hash(x) % n

    def mod_partition(self, x, n):
        """A partition function that partitions by modding the key.

        The mod partition function is useful if your keys are contiguous and you
        want to make sure that the partitions are sized equally.
        """
        return int(x) % n

    # The default partition function is hash_partition:
    partition = hash_partition

    def bypass(self):
        """Bypass implementation.

        The user may choose to use the bypass implementation instead of using
        MapReduce.
        """
        raise NotImplementedError

    def random(self, offset):
        """Creates a Random instance for MapReduce programs to use.

        The offset parameter allows programs to specify separate subsequences.
        Any two calls with the same offset will return Random instances that
        generate the same sequence of pseudo-random numbers.
        """
        import random
        import sys
        seed = int(self.opts.mrs__seed) + sys.maxint * offset
        return random.Random(seed)



# vim: et sw=4 sts=4

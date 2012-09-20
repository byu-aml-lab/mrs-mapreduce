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

"""Standard MapReduce programs.

This module consists of the MapReduce class, which provides the implementation
for a standard MapReduce program.  However, the MapReduce class can be extended
to create much more complex programs.
"""

from __future__ import division, print_function

import sys

from . import fileformats
from . import serializers
from .version import __version__

ITERATIVE_QMAX = 10
RAND_OFFSET_SHIFT = 64
SERIALIZERS = {'int': serializers.int_serializer,
        'str': serializers.str_serializer}

DEFAULT_USAGE = (""
"""%prog [OPTION]... INPUT_FILE... OUTPUT_DIR

Mrs Version """ + __version__ + """

The default implementation is Serial.  Note that you can give --help
separately for each implementation."""
)


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

    def input_data(self, job):
        """Returns an input_dataset.

        This is called by the default run method and is passed the job.
        Returns a dataset to be used for the input to the map function.  In
        case of a fatal error, None is returned.

        It may be helpful to access the list of command-line arguments in
        self.args.
        """
        if len(self.args) < 2:
            print("Requires input(s) and an output.", file=sys.stderr)
            return None
        inputs = self.args[:-1]
        return job.file_data(inputs)

    def output_dir(self):
        """Returns the name of the output directory.

        The output_directory is a string which determines where the
        output from the reduce function will be placed.  In case of a fatal
        error, None is returned.
        """
        if len(self.args) < 1:
            print("Requires an output.", file=sys.stderr)
            return None
        return self.args[-1]

    def run(self, job):
        """Default run which creates a map stage and a reduce stage."""
        source = self.input_data(job)
        if source is None:
            return 1
        outdir = self.output_dir()
        if outdir is None:
            return 1

        try:
            try:
                combiner = self.combine
            except AttributeError:
                combiner = None
            intermediate = job.map_data(source, self.map, combiner=combiner)
            source.close()
            output = job.reduce_data(intermediate, self.reduce,
                    outdir=outdir, format=fileformats.TextWriter)
            intermediate.close()
            output.close()

            ready = []
            while not ready:
                ready = job.wait(output, timeout=2.0)
                map_percent = 100 * job.progress(intermediate)
                reduce_percent = 100 * job.progress(output)
                print('Map: %.1f%% complete. Reduce: %.1f%% complete.'
                        % (map_percent, reduce_percent))
                sys.stdout.flush()
        except KeyboardInterrupt:
            print('Interrupted.')

        return 0

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

    def random(self, *offsets):
        """Creates a Random instance for MapReduce programs to use.

        The offset parameters allow programs to deterministically retrieve
        unique random number generators.  Each offset parameter is a number
        between 0 and 2**RAND_OFFSET_SHIFT (very big).  Every combination of
        offsets parameterizes a unique random number generator for a given
        value of --mrs-seed.  Note that a large number of offsets (about 300)
        can be given, and omitting a particular offset is equivalent to
        setting it to 0.
        """
        import random
        shift = 0
        seed = int(self.opts.mrs__seed)
        for x in offsets:
            shift += RAND_OFFSET_SHIFT
            seed += x << shift
        return random.Random(seed)

    def numpy_random(self, *offsets):
        """Creates a numpy.random.RandomState instance for MapReduce programs.

        See the `random()` method for details about the parameters.  Note
        that the rng given by `random()` is _not_ numerically equivalent.

        Note that unlike Python's random module, numpy does not accept big
        numbers, so we have to pass tuples.  This makes omitting an offset
        _not_ equivalent to setting it to 0.
        """
        import numpy
        return numpy.random.RandomState(offsets)

    @classmethod
    def update_parser(cls, parser):
        """Modify (and return) the given OptionParser."""
        parser.usage = DEFAULT_USAGE
        return parser

    def serializer(self, key):
        """Returns the Serializer associated with the given key."""
        return SERIALIZERS[key]


class IterativeMR(MapReduce):
    """A special type of MapReduce program optimized for iterative jobs.

    This class provides a run method that expects two user-provided methods:
        producer: submits datasets
        consumer: processes completed datasets
    """
    iterative_qmax = ITERATIVE_QMAX

    def run(self, job):
        """Default run which repeatedly calls producer and consumer."""
        datasets = set()
        while True:
            producer_active = True
            while producer_active and len(datasets) < self.iterative_qmax:
                new_datasets = self.producer(job)
                producer_active = bool(new_datasets)
                datasets.update(new_datasets)
                if not datasets:
                    return 0

            ready = job.wait(*datasets)
            for ds in ready:
                keep_going = self.consumer(ds)
                datasets.remove(ds)
                if not keep_going:
                    # TODO: job.abort()
                    return 0

    def producer(self, job):
        """Producer function.

        Submits one iteration of datasets for computation.  Called whenever
        the queue of submitted datasets begins to run low.

        Returns a list of new datasets that should be given to the consumer
        upon completion.
        """
        raise NotImplementedError

    def consumer(self, dataset):
        """Consumer function.

        Called whenever a dataset completes.  Prints output and/or determines
        whether stopping criteria have been met.  Returns True if execution
        should continue.
        """
        raise NotImplementedError


# vim: et sw=4 sts=4

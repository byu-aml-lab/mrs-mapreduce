# Mrs
# Copyright 2008-2011 Brigham Young University
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

import sys

from . import io

ITERATIVE_QMAX = 5


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
            print >>sys.stderr, "Requires input(s) and an output."
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
            print >>sys.stderr, "Requires an output."
            return None
        return self.args[-1]

    def run(self, job):
        """Default run which creates a map stage and a reduce stage."""
        source = self.input_data(job)
        if source is None:
            return False
        outdir = self.output_dir()
        if outdir is None:
            return False

        try:
            intermediate = job.map_data(source, self.map)
            source.close()
            output = job.reduce_data(intermediate, self.reduce, outdir=outdir,
                    format=io.TextWriter)
            intermediate.close()
            output.close()

            ready = []
            while not ready:
                ready = job.wait(output, timeout=2.0)
                map_percent = 100 * job.progress(intermediate)
                reduce_percent = 100 * job.progress(output)
                print ('Map: %.1f%% complete. Reduce: %.1f%% complete.'
                        % (map_percent, reduce_percent))
        except KeyboardInterrupt:
            print 'Interrupted.'

        return True

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
        seed = int(self.opts.mrs__seed) + sys.maxint * offset
        return random.Random(seed)


class IterativeMR(MapReduce):
    """A special type of MapReduce program optimized for iterative jobs.

    This class provides a run method that expects two user-provided methods:
        producer: submits datasets
        consumer: processes completed datasets
    """
    def run(self, job):
        """Default run which repeatedly calls producer and consumer."""
        datasets = set()
        while True:
            producer_active = True
            while producer_active and len(datasets) < ITERATIVE_QMAX:
                new_datasets = self.producer(job)
                producer_active = bool(new_datasets)
                datasets.update(new_datasets)
                if not datasets:
                    return True

            ready = job.wait(*datasets)
            for ds in ready:
                keep_going = self.consumer(ds)
                datasets.remove(ds)
                if not keep_going:
                    # TODO: job.abort()
                    return True

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

#!/usr/bin/env python

# Copyright 2008 Brigham Young University
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
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

VERSION = '0.1-pre'

IMPLEMENTATIONS = {}

def main(mapper, reducer):
    """Run a MapReduce program.

    Ideally, your Mrs MapReduce program looks something like this:

    def mapper(key, value):
        yield newkey, newvalue

    def reducer(key, value):
        yield newvalue

    if __name__ == '__main__':
        import mrs
        mrs.main(mapper, reducer)
    """
    from optparse import OptionParser
    import sys

    usage = 'usage: %prog implementation [args] input1 [input2 ...] output'
    version = 'Mrs %s' % VERSION

    parser = OptionParser(usage=usage)
    parser.add_option('--shared', dest='shared',
            help='Shared storage area (posix only)')
    parser.add_option('-M', '--map-tasks', dest='map_tasks', type='int',
            help='Number of map tasks (parallel only)')
    parser.add_option('-R', '--reduce-tasks', dest='reduce_tasks', type='int',
            help='Number of reduce tasks (parallel only)')
    parser.set_defaults(map_tasks=0, reduce_tasks=0)
    # TODO: other options:
    # input format
    # output format

    (options, args) = parser.parse_args()

    if len(args) < 3:
        parser.error("Requires an implementation, inputs, and an output.")

    try:
        implementation = IMPLEMENTATIONS[args[0]]
    except KeyError:
        parser.error("No such implementation exists.")
    inputs = args[1:-1]
    output = args[-1]

    implementation(mapper, reducer, inputs, output, options)


def add_implementation(implementation_list):
    def decorator(func):
        implementation_list[func.__name__] = func
        return func
    return decorator

@add_implementation(IMPLEMENTATIONS)
def posix(mapper, reducer, inputs, output, options):
    map_tasks = options.map_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1

    if options.map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    from mrs.mapreduce import Operation, POSIXJob
    op = Operation(mapper, reducer, map_tasks=map_tasks,
            reduce_tasks=options.reduce_tasks)
    mrsjob = POSIXJob(inputs, output, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()


@add_implementation(IMPLEMENTATIONS)
def serial(mapper, reducer, inputs, output, options):
    from mrs.mapreduce import Operation, SerialJob
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(inputs, output)
    mrsjob.operations = [op]
    mrsjob.run()


# vim: et sw=4 sts=4

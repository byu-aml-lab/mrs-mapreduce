#!/usr/bin/env python

VERSION = '0.1-pre'

from platforms import IMPLEMENTATIONS

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

    usage = 'usage: %prog implementation [args]'
    version = 'Mrs %s' % VERSION

    parser = OptionParser()
    parser.add_option('--shared', dest='shared',
            help='Shared storage area (posix only)')
    parser.add_option('-M', '--map-tasks', dest='map_tasks',
            help='Number of map tasks (parallel only)')
    parser.add_option('-R', '--reduce-tasks', dest='reduce_tasks',
            help='Number of reduce tasks (parallel only)')

    (options, args) = parser.parse_args()
    if not len(args):
        parser.error("No Mrs Implementation specified.")
    implementation = args[0]
    try:
        IMPLEMENTATIONS[args[0]](options, args)
    except KeyError:
        parser.error("No such implementation exists.")


# vim: et sw=4 sts=4

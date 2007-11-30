#!/usr/bin/env python
# TODO: fix the sample code in the following docstring:
"""MapReduce: a simple implementation (Mrs)

Your Mrs MapReduce program might look something like this:

def mapper(key, value):
    yield newkey, newvalue

def reducer(key, values):
    yield newvalue

if __name__ == '__main__':
    import mrs
    mrs.main(mapper, reducer)
"""

__all__ = ['main', 'Registry']

VERSION = '0.1-pre'
DEFAULT_RPC_PORT = 0

from registry import Registry

USAGE = (""
"""usage: %prog IMPLEMENTATION [OPTIONS] [ARG1 ...]
       %prog slave [OPTIONS] SERVER_URI

IMPLEMENTATION may be serial, master, or mockparallel.  A slave will attempt
to connect to a master listening at SERVER_URI.
""")

def main(registry, run=None, parser=None):
    """Run a MapReduce program.

    Requires a run function and a Registry.  If you want to, you can pass in
    an OptionParser instance called parser with your own custom options that
    we'll add to.  We'll overwrite the usage statement, but feel free to add
    an epilog.
    """
    from optparse import OptionParser
    import sys, os

    version = 'Mrs %s' % VERSION

    if parser is None:
        parser = OptionParser()
    parser.usage = USAGE
    parser.add_option('-p', '--port', dest='port', type='int',
            help='RPC Port for incoming requests')
    parser.add_option('--shared', dest='shared',
            help='Shared area for temporary storage (parallel only)')
    # TODO: allow -M to be specified: this will determine a default split
    #parser.add_option('-M', '--map-tasks', dest='map_tasks', type='int',
    #        help='Number of map tasks (parallel only)')
    parser.add_option('-R', '--reduce-tasks', dest='reduce_tasks', type='int',
            help='Number of reduce tasks (parallel only)')
    parser.set_defaults(map_tasks=0, reduce_tasks=1, port=DEFAULT_RPC_PORT,
            shared=os.getcwd())

    (options, raw_args) = parser.parse_args()
    if len(raw_args) < 1:
        parser.error("Requires an subcommand.")
    subcommand = raw_args[0]
    args = raw_args[1:]

    import mapreduce

    if run is None:
        run = mapreduce.mrs_simple

    if subcommand in ('master', 'slave'):
        import inspect
        frame = inspect.currentframe()
        try:
            prev_frame = frame.f_back
            filename = inspect.getfile(prev_frame)
            source = open(filename).read()
        except TypeError:
            print >>sys.stderr, ("Warning: couldn't open file for the frame"
                    "that called mrs.main()")
            source = ''
        finally:
            del frame
            del prev_frame
        registry.main_hash = str(hash(source))

        if subcommand == 'master':
            subcommand_args = (registry, run, args, options)
            from parallel import run_master
            subcommand_function = run_master
        elif subcommand == 'slave':
            if len(args) != 1:
                parser.error("Requires a server address and port.")
            uri = args[0]
            from slave import run_slave
            subcommand_function = run_slave
            subcommand_args = (registry, uri, options)
    elif subcommand in ('mockparallel', 'serial'):
        if len(args) < 2:
            parser.error("Requires inputs and an output.")
        inputs = args[0:-1]
        output = args[-1]
        subcommand_args = (registry, run, args, options)
        if subcommand == 'mockparallel':
            from serial import run_mockparallel
            subcommand_function = run_mockparallel
        elif subcommand == 'serial':
            from serial import run_serial
            subcommand_function = run_serial
    else:
        parser.error("No such subcommand exists.")

    try:
        retcode = subcommand_function(*subcommand_args)
    except KeyboardInterrupt:
        import sys
        print >>sys.stderr, "Interrupted."
        retcode = -1
    return retcode


# vim: et sw=4 sts=4

#!/usr/bin/env python

VERSION = '0.1-pre'
DEFAULT_RPC_PORT = 0

def main(mapper, reducer, partition=None):
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
    import sys, os

    usage = 'usage: %prog master-type [args] input1 [input2 ...] output\n' \
            '       %prog slave [args] server_uri'
    version = 'Mrs %s' % VERSION

    parser = OptionParser(usage=usage)
    parser.add_option('-p', '--port', dest='port', type='int',
            help='RPC Port for incoming requests')
    parser.add_option('--shared', dest='shared',
            help='Shared storage area (posix only)')
    parser.add_option('-M', '--map-tasks', dest='map_tasks', type='int',
            help='Number of map tasks (parallel only)')
    parser.add_option('-R', '--reduce-tasks', dest='reduce_tasks', type='int',
            help='Number of reduce tasks (parallel only)')
    parser.set_defaults(map_tasks=0, reduce_tasks=0, port=DEFAULT_RPC_PORT,
            shared=os.getcwd())

    (options, args) = parser.parse_args()
    if len(args) < 1:
        parser.error("Requires an subcommand.")
    subcommand = args[0]

    import mapreduce
    if partition is None:
        partition = mapreduce.default_partition
    mrs_prog = mapreduce.Program(mapper, reducer, partition)

    if subcommand == 'master':
        if len(args) < 3:
            parser.error("Requires inputs and an output.")
        inputs = args[1:-1]
        output = args[-1]
        subcommand_args = (mrs_prog, inputs, output, options)
        from parallel import run_master
        subcommand_function = run_master
    elif subcommand == 'slave':
        if len(args) != 2:
            parser.error("Requires a server address and port.")
        uri = args[1]
        from slave import run_slave
        subcommand_function = run_slave
        subcommand_args = (mrs_prog, uri, options)
    elif subcommand in ('posix', 'serial'):
        if len(args) < 3:
            parser.error("Requires inputs and an output.")
        inputs = args[1:-1]
        output = args[-1]
        subcommand_args = (mrs_prog, inputs, output, options)
        if subcommand == 'posix':
            from serial import run_posix
            subcommand_function = run_posix
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

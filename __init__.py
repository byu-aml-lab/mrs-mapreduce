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
SLAVE_PORT = 0
COOKIE_LEN = 64

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

    usage = 'usage: %prog master-type [args] input1 [input2 ...] output\n' \
            '       %prog slave [args] server_uri'
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
    if len(args) < 1:
        parser.error("Requires an subcommand.")
    subcommand = args[0]

    if subcommand in ('posix', 'serial'):
        if len(args) < 3:
            parser.error("Requires inputs and an output.")
        inputs = args[1:-1]
        output = args[-1]
        if subcommand == 'posix':
            retcode = posix(mapper, reducer, inputs, output, options)
        elif subcommand == 'serial':
            retcode = serial(mapper, reducer, inputs, output, options)
    elif subcommand == 'slave':
        if len(args) != 2:
            parser.error("Requires a server address and port.")
        uri = args[1]
        retcode = slave(master, reducer, uri)
    else:
        parser.error("No such subcommand exists.")

    return retcode


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
    return 0


def serial(mapper, reducer, inputs, output, options):
    from mrs.mapreduce import Operation, SerialJob
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(inputs, output)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0

def slave(mapper, reducer, uri):
    """Mrs Slave

    uri is scheme://host/target and may include username:password
    """
    import slave, rpc
    import random, string, xmlrpclib

    # Create an RPC proxy to the master's RPC Server
    master = xmlrpclib.ServerProxy(uri)

    # Startup a slave RPC Server
    server = rpc.new_server(slave.SlaveRPC, SLAVE_PORT)
    host, port = server.server_address

    # Generate a cookie, so that mostly only authorized people can talk to us.
    possible = string.letters + string.digits
    cookie = ''.join(random.choice(possible) for i in xrange(COOKIE_LEN))

    try:
        # TODO: start a worker thread
        # TODO: sign in with the master
        master.signin(cookie, port)
        while True:
            server.handle_request()
    except KeyboardInterrupt:
        return -1

# vim: et sw=4 sts=4

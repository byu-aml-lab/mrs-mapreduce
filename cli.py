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

from version import VERSION

USAGE = (""
"""%prog IMPLEMENTATION [OPTIONS] [ARGS]

Mrs Version """ + VERSION + """

The subcommand IMPLEMENTATION must be the first argument and can be "master",
"slave", "serial", or "mock_parallel".  Note that you can give --help
separately for each subcommand."""
)


def main(registry, run=None, setup=None, update_parser=None):
    """Run a MapReduce program.

    Requires a run function and a Registry.
    
    If setup is provided, it will be called before performing any work, and
    all command-line options will be passed in.
    
    If you want to modify the basic Mrs Parser, provide an update_parser
    function that takes a parser and either modifies it or returns a new one.
    The parser will be given all options/arguments except the Mrs
    Implementation.  Note that no option should ever have the value None.
    """
    parser = option_parser()
    import sys
    if len(sys.argv) < 2:
        parser.error("Mrs Implementation must be specified.")

    mrs_impl = sys.argv[1]

    if run is None:
        from run import mrs_simple
        run = mrs_simple

    if mrs_impl in ('-h', '--help'):
        # It's not a Mrs Implementation, but try to help anyway.
        parser.print_help()
        return
    elif mrs_impl == 'master':
        impl_function = run_master
        add_master_options(parser)
        if update_parser:
            parser = update_parser(parser)
    elif mrs_impl == 'slave':
        impl_function = run_slave
        add_slave_options(parser)
    elif mrs_impl == 'mockparallel':
        impl_function = run_mockparallel
        add_master_options(parser)
        if update_parser:
            parser = update_parser(parser)
    elif mrs_impl == 'serial':
        raise NotImplementedError('The serial implementation is '
                'temporarily broken.  Sorry.')
        impl_function = run_serial
        add_master_options(parser)
        if update_parser:
            parser = update_parser(parser)
    else:
        parser.error("Invalid Mrs Implementation: %s" % mrs_impl)

    (options, args) = parser.parse_args(sys.argv[2:])

    try:
        retcode = impl_function(registry, run, setup, args, options)
    except KeyboardInterrupt:
        import sys
        print >>sys.stderr, "Interrupted."
        retcode = -1
    return retcode


def primary_impl(impl):
    """Report whether the given implementation is a "main" one or a slave."""

    return impl in ('master', 'mockparallel', 'serial')


def option_parser():
    """Create the default Mrs Parser

    The parser is an optparse.OptionParser.  It is configured to use the
    resolve conflict_handler, so any option can be overridden simply by
    defining a new option with the same option string.  The remove_option and
    get_option methods still work, too.  Note that overriding an option only
    shadows it while still allowing its other option strings to work, but
    remove_option completely removes the option with all of its option
    strings.

    The usage string can be specified with set_usage, thus overriding the
    default.  However, often what you really want to set is the epilog.  The
    usage shows up in the help before the option list; the epilog appears
    after.
    """
    import optparse

    parser = optparse.OptionParser(conflict_handler='resolve')
    parser.usage = USAGE

    parser.add_option('-P', '--mrs-port', dest='mrs_port', type='int',
            help='RPC Port for incoming requests')
    parser.set_defaults(mrs_port=0)

    return parser


def add_master_options(parser):
    import os
    default_shared = os.getcwd()
    parser.add_option('-S', '--mrs-shared', dest='mrs_shared',
            help='Shared area for temporary storage (parallel only)')
    parser.add_option('-R', '--mrs-reduce-tasks', dest='mrs_reduce_tasks',
            type='int', help='Default number of reduce tasks (parallel only)')
    parser.add_option('--mrs-runfile', dest='mrs_runfile',
            help="Server's RPC port will be written here (parallel only)")
    parser.set_defaults(mrs_reduce_tasks=1, mrs_shared=default_shared)


def add_slave_options(parser):
    parser.add_option('-M', '--mrs-master', dest='mrs_master',
            help='URL of the Master RPC server (slave only)')


# FIXME:
def run_serial(registry, user_run, user_setup, args, opts):
    """Mrs Serial
    """
    from serial import Serial
    from job import Job

    # Create Job
    job = Job(registry, jobdir, user_run, user_setup, args, opts)

    # TODO: later, don't assume that this is a short-running function:
    job.run()

    # TODO: this should spin off as another thread while job runs in the
    # current thread:
    mrs_exec = Serial(job, registry)
    mrs_exec.run()
    return 0

def run_mockparallel(registry, user_run, user_setup, args, opts):
    from serial import MockParallel
    from job import Job

    # Set up job directory
    shared_dir = opts.mrs_shared
    from util import try_makedirs
    try_makedirs(shared_dir)
    import tempfile
    jobdir = tempfile.mkdtemp(prefix='mrs.job_', dir=shared_dir)

    # Create Job
    job = Job(registry, jobdir, user_run, user_setup, args, opts)

    mrs_exec = MockParallel(job, registry, opts)
    mrs_exec.run()
    return 0

def run_master(registry, user_run, user_setup, args, opts):
    """Mrs Master
    """
    from parallel import Parallel
    from job import Job

    # Set up job directory
    shared_dir = opts.mrs_shared
    from util import try_makedirs
    try_makedirs(shared_dir)
    import tempfile
    jobdir = tempfile.mkdtemp(prefix='mrs.job_', dir=shared_dir)

    # Create Job
    job = Job(registry, jobdir, user_run, user_setup, args, opts)

    mrs_exec = Parallel(job, registry, opts)
    mrs_exec.run()
    return 0

def run_slave(registry, user_run, user_setup, args, opts):
    """Mrs Slave

    The uri is of the form scheme://username:password@host/target with
    username and password possibly omitted.
    """
    from slave import do_stuff

    do_stuff(registry, user_setup, opts)

    return 0


# vim: et sw=4 sts=4

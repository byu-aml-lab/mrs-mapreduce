#!/usr/bin/env python

# Mrs Fulton -- run Mrs programs on Marylou4 (BYU's supercomputer)
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

"""Script for submitting jobs to a PBS scheduler.

This file is not meant to be a completely working example; We've used it to
submit MapReduce jobs to Brigham Young University's Fulton Supercomputing Lab.
It is included just as a helpful example.
"""

# TODO: make the number of slaves per job default to '0' (all-in-one) and
# also include the master in the same job as the first batch of slaves.


from __future__ import division

DEFAULT_INTERPRETER = "/usr/bin/python"
INTERFACES = "ib0 eth1 eth0"
RUN_DIRECTORY = "$HOME/compute/run"

QSUB_NAME_DEFAULT = "mrs_fulton"
QSUB_NAME_MAXLEN = 15
QSUB_NAME_MASTER = "_M"
QSUB_NAME_SLAVE = "_S"

import sys, os
from subprocess import Popen, PIPE

def main():
    parser = create_parser()
    options, args = parser.parse_args()

    suffix_len = max(len(QSUB_NAME_MASTER), len(QSUB_NAME_SLAVE))
    if len(options.name) + suffix_len <= QSUB_NAME_MAXLEN:
        name = options.name
    else:
        parser.error('NAME is too long.')

    if options.output is None:
        parser.error('OUTPUT file must be specified')

    if options.time is None:
        parser.error('TIME must be specified')

    if options.memory is None:
        parser.error('MEMORY must be specified')

    # Extract Mrs program and its command-line arguments/options
    if len(args) >= 1:
        mrs_program = args[0]
        mrs_args = args[1:]
    else:
        parser.error('MRS_PROGRAM not specified.')

    if not options.force and os.path.exists(options.output):
        print >>sys.stderr, "Output file already exists:", options.output
        sys.exit(-1)

    # Set up the job directory for output, etc.
    jobdir_raw = os.path.join(RUN_DIRECTORY, name)
    jobdir = os.path.expandvars(jobdir_raw)
    try:
        os.makedirs(jobdir)
    except OSError:
        # The directory might already exist.
        pass

    # Common command line arguments to qsub:
    time = walltime(options.time)
    nodespec = 'nodes=1:ppn=1'
    if options.nodespec:
        nodespec = '%s:%s' % (nodespec, options.nodespec)
    resources = '%s,walltime=%s,pmem=%sgb' % (nodespec, time, options.memory)
    if options.resource_list:
        resources = '%s,%s' % (resources, options.resource_list)
    singleproc_cmdline = ['qsub', '-l', resources]
    # TODO: set 'synccount' on the master and set some slaves to 'syncwith'
    # the master.
    # TODO: when each slave is able to use multiple processors (with multiple
    # worker subprocesses), change `ppn` accordingly.
    nodespec = 'nodes=%s:ppn=1' % options.slaves_per_job
    if options.nodespec:
        nodespec = '%s:%s' % (nodespec, options.nodespec)
    resources = '%s,walltime=%s,pmem=%sgb' % (nodespec, time, options.memory)
    if options.resource_list:
        resources = '%s,%s' % (resources, options.resource_list)
    multiproc_cmdline = ['qsub', '-l', resources]

    # Variables for the job script:
    current_dir = os.getcwd()
    quoted_args = ['"%s"' % arg.replace('"', r'\"') for arg in mrs_args]
    arg_array = "(%s)" % " ".join(quoted_args)
    script_vars = dict(python=options.interpreter, program=mrs_program,
            arg_array=arg_array, interfaces=INTERFACES, jobdir=jobdir,
            current_dir=current_dir, output=options.output)

    print "Submitting master job...",
    jobid = submit_master(name, script_vars, singleproc_cmdline, jobdir)
    print " done."
    print "Master jobid:", jobid

    script_vars['master_jobid'] = jobid

    print "Submitting slave jobs...",
    for i in xrange(options.nslaves // options.slaves_per_job):
        submit_slavejob(i, name, script_vars, multiproc_cmdline, jobdir, jobid)
    for i in xrange(options.nslaves % options.slaves_per_job):
        submit_slavejob(i, name, script_vars, singleproc_cmdline, jobdir, jobid)


def submit_master(name, script_vars, cmdline, jobdir):
    """Submit the master to PBS using qsub.

    Returns the jobid of the newly created job.
    """
    script = r'''#!/bin/bash
        . $HOME/.bashrc

        # Output redirection will fail if the file already exists:
        set noclobber

        cd "%(current_dir)s"

        JOBDIR="%(jobdir)s"
        PYTHON="%(python)s"
        MRS_PROGRAM="%(program)s"
        ARGS=%(arg_array)s
        INTERFACES="%(interfaces)s"
        OUTPUT="%(output)s"

        HOST_FILE="$JOBDIR/host.$PBS_JOBID"
        PORT_FILE="$JOBDIR/port.$PBS_JOBID"
        STDERR_FILE="$JOBDIR/master-stderr.$PBS_JOBID"

        # Run /sbin/ip and extract everything between "inet " and "/" (i.e.
        # the IP address but not the netmask).  Note that we use a semi-colon
        # instead of / in the sed expression to make it easier on the eyes.
        for iface in $INTERFACES; do
            if /sbin/ip -o -4 addr list |grep -q "$iface\$"; then
                IP_ADDRESS=$(/sbin/ip -o -4 addr list "$iface" \
                        |sed -e 's;^.*inet \(.*\)/.*$;\1;')
                echo $IP_ADDRESS >$HOST_FILE
                break
            fi
        done
        if [[ -z $IP_ADDRESS ]]; then
            echo "No valid IP address found!"
            exit 1
        fi

        # Master
        mkdir -p $(dirname "$OUTPUT")
        $PYTHON $MRS_PROGRAM --mrs=Master --mrs-runfile="$PORT_FILE" \
                ${ARGS[@]} >$OUTPUT 2>$STDERR_FILE
        ''' % script_vars

    cmdline = list(cmdline)
    cmdline += ['-N', name + QSUB_NAME_MASTER]
    outfile = os.path.join(jobdir, 'pbs_master_stdout')
    errfile = os.path.join(jobdir, 'pbs_master_stderr')
    cmdline += ['-o', outfile, '-e', errfile]

    # Submit
    qsub_proc = Popen(cmdline, stdin=PIPE, stdout=PIPE)

    stdout, stderr = qsub_proc.communicate(script)
    if qsub_proc.returncode != 0:
        print >>sys.stderr, "Couldn't submit master job to queue!"
        sys.exit(-1)
    jobid = stdout.strip()
    return jobid


def submit_slavejob(i, name, script_vars, cmdline, jobdir, master_jobid):
    """Submit a single slave job to PBS using qsub."""

    script = r'''#!/bin/bash
        . $HOME/.bashrc

        cd "%(current_dir)s"

        JOBDIR="%(jobdir)s"
        PYTHON="%(python)s"
        MRS_PROGRAM="%(program)s"
        MASTER_JOBID="%(master_jobid)s"

        HOST_FILE="$JOBDIR/host.$MASTER_JOBID"
        PORT_FILE="$JOBDIR/port.$MASTER_JOBID"

        # Slave
        while true; do
            if [[ -e $PORT_FILE ]]; then
                PORT=$(cat $PORT_FILE)
                if [[ $PORT = "-" ]]; then
                    echo "The master quit prematurely."
                    exit
                elif [[ ! -z $PORT ]]; then
                    break;
                fi
            fi
            sleep 0.05;
        done
        HOST=$(cat $HOST_FILE)

        echo "Connecting to master on '$HOST:$PORT'"

        pbsdsh bash -i \
            -c "$PYTHON $MRS_PROGRAM --mrs=Slave --mrs-master='$HOST:$PORT'"
        ''' % script_vars

    # Don't print jobid to stdout
    cmdline = list(cmdline)
    cmdline += ['-z']
    # Set the job name
    cmdline += ['-N', name + QSUB_NAME_SLAVE]
    # Start after the master starts:
    #dependency = 'after:%s' % master_jobid
    #cmdline += ['-W', 'depend=%s' % dependency]
    # Set stdout and stderr:
    outfile = os.path.join(jobdir, 'slave-job-%s.out' % i)
    errfile = os.path.join(jobdir, 'slave-job-%s.err' % i)
    cmdline += ['-o', outfile, '-e', errfile]

    # Submit
    qsub_proc = Popen(cmdline, stdin=PIPE, stdout=PIPE)

    stdout, stderr = qsub_proc.communicate(script)
    if qsub_proc.wait() != 0:
        print >>sys.stderr, "Couldn't submit slave job to queue!"
        sys.exit(-1)


def walltime(time):
    """Return a qsub-style walltime string for the given time (in hours)."""
    hours = int(time)
    time -= hours
    minutes = int(time * 60)
    time -= minutes / 60
    seconds = int(time * 3600)
    return ":".join(map(str, (hours, minutes, seconds)))


USAGE = ("""%prog [OPTIONS] -- MRS_PROGRAM [PROGRAM_OPTIONS]

Mrs Fulton uses qsub to submit a Mrs program to the supercomputer.  The given
MRS_PROGRAM runs with the given PROGRAM_OPTIONS.  These options should not
include master or slave subcommands, since Mrs Fulton will take care of these
details.""")

def create_parser():
    from optparse import OptionParser
    parser = OptionParser(usage=USAGE)
    # We don't want options intended for the Mrs Program to go to Mrs Fulton.
    parser.disable_interspersed_args()

    parser.add_option('-n', dest='nslaves', type='int',
            help='Number of slaves')
    parser.add_option('-N', '--name', dest='name', help='Name of job')
    parser.add_option('-o', '--output', dest='output', help='Output directory')
    parser.add_option('-t', '--time', dest='time', type='float',
            help='Wallclock time (in hours)')
    parser.add_option('-m', '--memory', dest='memory', type='int',
            help='Amount of memory per node (in GB)')
    parser.add_option('-s', dest='slaves_per_job', type='int',
            help='Number of slaves in each PBS job', default=10)
    parser.add_option('--interpreter', dest='interpreter', action='store',
            help='Python interpreter to run', default=DEFAULT_INTERPRETER)
    parser.add_option('-f', dest='force', action='store_true',
            help='Force output, even if the output file already exists')
    parser.add_option('--nodespec', dest='nodespec',
            help='Extra node spec options (colon-separated PBS syntax)')
    parser.add_option('-l', '--resource-list', dest='resource_list',
            help='Extra resource requests (comma-separated PBS syntax)')

    parser.set_defaults(n=1, name=QSUB_NAME_DEFAULT)
    return parser

if __name__ == '__main__':
    main()

# vim: et sw=4 sts=4

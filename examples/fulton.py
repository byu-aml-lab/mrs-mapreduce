#!/usr/bin/env python

# Mrs Fulton -- run Mrs programs on Marylou4 (BYU's supercomputer)
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

from __future__ import division

PYTHON="/fslapps/Python-2.6.2/bin/python2.6"
#PYTHON = "python2.6"
INTERFACE = "eth1"
RUN_DIRECTORY = "$HOME/compute/run"

QSUB_NAME_DEFAULT = "mrs_fulton"
QSUB_NAME_LEN = 15
QSUB_NAME_MASTER = "_M"
QSUB_NAME_SLAVE = "_S"

import sys, os
from subprocess import Popen, PIPE

def main():
    parser = create_parser()
    options, args = parser.parse_args()

    max_len = QSUB_NAME_LEN - max(len(QSUB_NAME_MASTER), len(QSUB_NAME_SLAVE))
    if len(options.name) <= max_len:
        name = options.name
    else:
        parser.error('NAME is too long.')

    if options.output is None:
        parser.error('OUTPUT file must be specified')

    if options.time is None:
        parser.error('TIME must be specified')

    # Extract Mrs program and its command-line arguments/options
    if len(args) >= 1:
        mrs_program = args[0]
        mrs_args = args[1:]
    else:
        parser.error('MRS_PROGRAM not specified.')

    if os.path.exists(options.output):
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
    cmdline = ['qsub', '-l', 'nodes=1:ppn=1,walltime=%s' % time]

    # Variables for the job script:
    current_dir = os.getcwd()
    quoted_args = ['"%s"' % arg.replace('"', r'\"') for arg in mrs_args]
    arg_array = "(%s)" % " ".join(quoted_args)
    script_vars = dict(python=PYTHON, program=mrs_program,
            arg_array=arg_array, interface=INTERFACE, jobdir=jobdir,
            current_dir=current_dir, output=options.output,
            timeout=options.timeout)

    print "Submitting master job...",
    jobid = submit_master(name, script_vars, cmdline, jobdir)
    print " done."
    print "Master jobid:", jobid

    script_vars['master_jobid'] = jobid

    for i in xrange(options.n):
        submit_slave(name, script_vars, cmdline, jobdir, jobid)


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
        INTERFACE="%(interface)s"
        OUTPUT="%(output)s"
        TIMEOUT="%(timeout)s"

        HOST_FILE="$JOBDIR/host.$PBS_JOBID"
        PORT_FILE="$JOBDIR/port.$PBS_JOBID"

        # Run /sbin/ip and extract everything between "inet " and "/" (i.e.
        # the IP address but not the netmask).  Note that we use a semi-colon
        # instead of / in the sed expression to make it easier on the eyes.
        IP_ADDRESS=$(/sbin/ip -o -4 addr list "$INTERFACE" \
                |sed -e 's;^.*inet \(.*\)/.*$;\1;')
        echo $IP_ADDRESS >$HOST_FILE

        # Master
        $PYTHON $MRS_PROGRAM --mrs=Master --mrs-shared="$JOBDIR" \
                --mrs-runfile="$PORT_FILE" --mrs-timeout="$TIMEOUT" \
                --mrs-pingdelay="$TIMEOUT" ${ARGS[@]} >$OUTPUT
        ''' % script_vars

    cmdline += ['-N', name + QSUB_NAME_MASTER]
    outfile = os.path.join(jobdir, 'master_stdout')
    errfile = os.path.join(jobdir, 'master_stderr')
    cmdline += ['-o', outfile, '-e', errfile]

    # Submit
    qsub_proc = Popen(cmdline, stdin=PIPE, stdout=PIPE)

    stdout, stderr = qsub_proc.communicate(script)
    if qsub_proc.returncode != 0:
        print >>sys.stderr, "Couldn't submit master job to queue!"
        sys.exit(-1)
    jobid = stdout.strip()
    return jobid


def submit_slave(name, script_vars, cmdline, jobdir, master_jobid):
    """Submit a single slave to PBS using qsub."""

    script = r'''#!/bin/bash
        . $HOME/.bashrc

        cd "%(current_dir)s"

        JOBDIR="%(jobdir)s"
        PYTHON="%(python)s"
        MRS_PROGRAM="%(program)s"
        MASTER_JOBID="%(master_jobid)s"
        TIMEOUT="%(timeout)s"

        HOST_FILE="$JOBDIR/host.$MASTER_JOBID"
        PORT_FILE="$JOBDIR/port.$MASTER_JOBID"

        # Slave
        while [[ ! -e $PORT_FILE ]]; do sleep 1; done
        PORT=$(cat $PORT_FILE)
        HOST=$(cat $HOST_FILE)
        $PYTHON $MRS_PROGRAM --mrs=Slave --mrs-master="$HOST:$PORT" \
                --mrs-timeout="$TIMEOUT" --mrs-pingdelay="$TIMEOUT"
        ''' % script_vars

    # Don't print jobid to stdout
    cmdline += ['-z']
    # Set the job name
    cmdline += ['-N', name + QSUB_NAME_SLAVE]
    # Start after the master starts:
    #dependency = 'after:%s' % master_jobid
    #cmdline += ['-W', 'depend=%s' % dependency]
    # Set stdout and stderr:
    outfile = os.path.join(jobdir, 'slave_stdout')
    errfile = os.path.join(jobdir, 'slave_stderr')
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


USAGE = (""
"""%prog [OPTIONS] -- MRS_PROGRAM [PROGRAM_OPTIONS]

Mrs Fulton uses qsub to submit a Mrs program to the supercomputer.  The given
MRS_PROGRAM runs with the given PROGRAM_OPTIONS.  These options should not
include master or slave subcommands, since Mrs Fulton will take care of these
details.""")

def create_parser():
    from optparse import OptionParser
    parser = OptionParser(usage=USAGE)
    # We don't want options intended for the Mrs Program to go to Mrs Fulton.
    parser.disable_interspersed_args()

    parser.add_option('-n', dest='n', help='Number of slaves', type='int')
    parser.add_option('-N', '--name', dest='name', help='Name of job')
    parser.add_option('-o', '--output', dest='output', help='Output directory')
    parser.add_option('--timeout', dest='timeout',
            help='Timeout for RPC calls and pings')
    parser.add_option('-t', '--time', dest='time', type='float',
            help='Wallclock time (in hours)')

    parser.set_defaults(n=1, name=QSUB_NAME_DEFAULT)
    return parser

if __name__ == '__main__':
    main()

# vim: et sw=4 sts=4

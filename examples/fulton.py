#!/usr/bin/env python
# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Script for submitting jobs to a PBS scheduler.

This file is not meant to be a completely working example; We've used it to
submit MapReduce jobs to Brigham Young University's Fulton Supercomputing Lab.
It is included just as a helpful example.
"""

# TODO: make the number of slaves per job default to '0' (all-in-one) and
# also include the master in the same job as the first batch of slaves.


from __future__ import division

import math
import os
from subprocess import Popen, PIPE
import sys

DEFAULT_INTERPRETER = "/usr/bin/python"
INTERFACES = "ib0 eth1 eth0"
RUN_DIRECTORY = "$HOME/compute/run"

QSUB_NAME_DEFAULT = "mrs_fulton"
QSUB_NAME_MAXLEN = 15


def main():
    parser = create_parser()
    opts, args = parser.parse_args()

    if opts.slaves_per_job > 0:
        total_jobs = math.ceil((1 + opts.nslaves) / opts.slaves_per_job)
        suffix_len = len('_') + int(1 + math.log10(total_jobs))
    else:
        suffix_len = len('_') + 1
    if len(opts.name) + suffix_len <= QSUB_NAME_MAXLEN:
        name = opts.name
    else:
        parser.error('NAME is too long.')

    if opts.output is None:
        parser.error('OUTPUT file must be specified')

    if opts.time is None:
        parser.error('TIME must be specified')

    if opts.memory is None:
        parser.error('MEMORY must be specified')

    # Extract Mrs program and its command-line arguments/options
    if len(args) >= 1:
        mrs_program = args[0]
        mrs_args = args[1:]
    else:
        parser.error('MRS_PROGRAM not specified.')

    if not opts.force and os.path.exists(opts.output):
        print >>sys.stderr, "Output file already exists:", opts.output
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
    time = walltime(opts.time)
    # TODO: when each slave is able to use multiple processors (with multiple
    # worker subprocesses), change `ppn` accordingly.
    #nodespec = 'nodes=%s:ppn=1'
    nodespec = 'procs=%s'
    if opts.nodespec:
        nodespec = '%s:%s' % (nodespec, opts.nodespec)
    resources = '%s,walltime=%s,pmem=%smb' % (nodespec, time, opts.memory)
    if opts.resource_list:
        resources += ',%s' % opts.resource_list

    # Variables for the job script:
    current_dir = os.getcwd()
    quoted_args = ['"%s"' % arg.replace('"', r'\"') for arg in mrs_args]
    arg_array = "(%s)" % " ".join(quoted_args)
    script_vars = {
            'python': opts.interpreter,
            'program': mrs_program,
            'arg_array': arg_array,
            'interfaces': INTERFACES,
            'jobdir': jobdir,
            'current_dir': current_dir,
            'output': opts.output,
            'stderr': opts.stderr,
            'master_jobid': '',
            }

    if opts.slaves_per_job > 0:
        nodes = min(1 + opts.nslaves, opts.slaves_per_job)
    else:
        nodes = 1 + opts.nslaves
    print "Submitting master job...",
    jobid = submit_job('%s_0' % name, script_vars, jobdir, resources % nodes)
    print " done."
    nodes_left = 1 + opts.nslaves - nodes

    print "Master jobid:", jobid
    script_vars['master_jobid'] = jobid
    attrs = 'depend=after:%s' % jobid

    print "Submitting slave jobs...",
    i = 1
    while nodes_left > 0:
        nodes = min(nodes_left, opts.slaves_per_job)
        submit_job('%s_%s' % (name, i), script_vars, jobdir,
                resources % nodes, attrs)
        nodes_left -= nodes
        i += 1


def submit_job(name, script_vars, jobdir, resources, attrs=''):
    """Submit a single job to PBS using qsub.

    Returns the jobid of the newly created job.  If `master_jobid` (in the
    `script_vars`) is an empty string, then the new job will include a master.
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
        MASTER_JOBID="%(master_jobid)s"
        INTERFACES="%(interfaces)s"
        OUTPUT="%(output)s"
        STDERR="%(stderr)s"

        if [[ -z $MASTER_JOBID ]]; then
            HOST_FILE="$JOBDIR/host.$PBS_JOBID"
            PORT_FILE="$JOBDIR/port.$PBS_JOBID"

            # Run /sbin/ip and extract everything between "inet " and "/"
            # (i.e.  the IP address but not the netmask).  Note that we use a
            # semi-colon instead of / in the sed expression to make it easier
            # on the eyes.
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

            # Start the master.
            mkdir -p $(dirname "$OUTPUT")
            if [[ -n $STDERR ]]; then
                mkdir -p $(dirname "$STDERR")
            else
                STDERR="$JOBDIR/master-stderr.$PBS_JOBID"
            fi
            $PYTHON $MRS_PROGRAM --mrs=Master --mrs-runfile="$PORT_FILE" \
                    ${ARGS[@]} >$OUTPUT 2>$STDERR &
        else
            HOST_FILE="$JOBDIR/host.$PBS_MASTER_JOBID"
            PORT_FILE="$JOBDIR/port.$PBS_MASTER_JOBID"
        fi

        # Find the port used by the master.
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

        # Start the slaves.
        SLAVE_CMD="$PYTHON $MRS_PROGRAM --mrs=Slave --mrs-master='$HOST:$PORT'"
        if [[ -z $MASTER_JOBID ]]; then
            # Don't start a slave on the master's node.
            SLAVE_CMD="[[ \$PBS_VNODENUM != 0 ]] && $SLAVE_CMD"
        fi
        pbsdsh bash -i -c "$SLAVE_CMD"

        # Wait for the master (backgrounded) to complete.
        wait
        ''' % script_vars

    cmdline = ['qsub', '-l', resources, '-N', name]
    if attrs:
        cmdline += ['-W', attrs]
    outfile = os.path.join(jobdir, '%s.out' % name)
    errfile = os.path.join(jobdir, '%s.err' % name)
    cmdline += ['-o', outfile, '-e', errfile]

    # Submit
    qsub_proc = Popen(cmdline, stdin=PIPE, stdout=PIPE)

    stdout, stderr = qsub_proc.communicate(script)
    if qsub_proc.returncode != 0:
        print >>sys.stderr, "Couldn't submit master job to queue!"
        sys.exit(-1)
    jobid = stdout.strip()
    return jobid


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
    parser.add_option('-o', '--output', dest='output',
            help='Output (stdout) file')
    parser.add_option('-e', '--stderr', dest='stderr', default='',
            help='Output (stderr) file')
    parser.add_option('-t', '--time', dest='time', type='float',
            help='Wallclock time (in hours)')
    parser.add_option('-m', '--memory', dest='memory', type='int',
            help='Amount of memory per node (in MB)')
    parser.add_option('-s', dest='slaves_per_job', type='int',
            help='Number of slaves in each PBS job', default=0)
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

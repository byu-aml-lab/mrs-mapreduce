#!/usr/bin/python
# Mrs
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

"""Script for submitting jobs using pssh (parallel-ssh)."""


from __future__ import division, print_function

import getpass
import math
import optparse
import os
import socket
import subprocess
import sys
import time

DESCRIPTION = 'Runs a Mrs program using screen and pssh.'
USAGE = '%prog --hosts slave_hosts_file mrs_program [args]'
EPILOG = """
The 'slave_hosts_file' will be passed to pssh and should be a text file
with the name of a slave machine on each line in the format
"[user@][host][:port]".

If the user name is left off, pssh will use the current user name, and
for the port number, the ssh default will be used (port 22).

Note that you will need to set up passphraseless ssh to the master
and slave machines before running this script.

All output is put in a folder named after the jobname.
"""

DEFAULT_OUTPUT_DIR = os.getcwd() # set default to current working directory
DEFAULT_LOCAL_SCRATCH = getpass.getuser()
DEFAULT_JOBNAME = 'newjob'

# Note that screen only seems to be able to paste 492 bytes at a time. Why??
SCREEN_STUFF_MAX = 200

##############################################################################
# Setup

# Here we use python's option parser to setup any run options we may want.
# They can be printed out with the --help option from the command line:
# example: $ python clusterrun.py --help
parser = optparse.OptionParser(conflict_handler='resolve',
        usage=USAGE, description=DESCRIPTION, epilog=EPILOG)
parser.disable_interspersed_args()

parser.add_option('-h', '--hosts',
        dest='hostfiles',
        action='append',
        help='Hosts file (each line "[user@][hostname][:port]")',
        default=[])
parser.add_option('-n', '--jobname',
        dest='jobname',
        action='store',
        help='Job name. Default is \'newjob\'',
        default=DEFAULT_JOBNAME)
parser.add_option('-o', '--outdir',
        dest='out_dir',
        action='store',
        help='Output directory. Default is current working directory',
        default=DEFAULT_OUTPUT_DIR)
parser.add_option('--interpreter',
        dest='interpreter',
        action='store',
        help='Python interpreter to run',
        default='python')
parser.add_option('--mrs-tmpdir',
        dest='mrs_tmpdir',
        action='store',
        help='local temporary directory',
        default='/tmp')
parser.add_option('--mrs-profile',
        dest='mrs_profile',
        action='store_true',
        help='run mrs under a profiler',
        default=False)
parser.add_option('--mrs-verbose',
        dest='mrs_verbose',
        action='store_true',
        help='run mrs in verbose mode',
        default=False)
parser.add_option('--mrs-debug',
        dest='mrs_debug',
        action='store_true',
        help='run mrs in debug mode',
        default=False)
parser.add_option('--background',
                  dest='background',
                  action='store_true',
                  help='run mrs in the background',
                  default=False)

opts, args = parser.parse_args()

# Make sure slave machines are specified.
if not opts.hostfiles:
    print('No hosts file specified!', file=sys.stderr)
    sys.exit(1)

# Make sure that pssh and screen are installed
if subprocess.call(('which', 'pssh'), stdout=open('/dev/null', 'w')):
    print('Error: Pssh not installed!', file=sys.stderr)
    sys.exit(1)
if subprocess.call(('which', 'screen'), stdout=open('/dev/null', 'w')):
    print('Error: Screen not installed!', file=sys.stderr)
    sys.exit(1)

# Initalize any needed variables.
mrs_program = args[0] # get name of Mrs program
mrs_argv = args[1:] # get input file
job_dir = os.path.join(opts.out_dir, opts.jobname)
runfilename = os.path.join(job_dir, 'master.run') # this will have the port num
timefilename = os.path.join(job_dir, 'time.txt')
host_options = ' '.join('--hosts %s' % hostfile for hostfile in opts.hostfiles)
master_hostname = socket.gethostname()

# Check if the job directory already exists
if os.path.exists(job_dir):
    print('Job directory already exists!')
    sys.exit(1)

# This method will be called to run commands on the command line.
def run(*args):
    returncode = subprocess.call(args)
    if returncode != 0:
        print('Command failed with error code', returncode, file=sys.stderr)
        sys.exit(1)

def stuff_to_screen(screen_name, window, string):
    """Send a string to an open screen session.

    Screen ignores strings longer than a certain length, so we split the
    input into smaller pieces.
    """
    chunks = int(math.ceil(len(string) / SCREEN_STUFF_MAX))
    for i in range(chunks):
        chunk_string = string[i * SCREEN_STUFF_MAX: (i + 1) * SCREEN_STUFF_MAX]
        run('screen', '-S', screen_name, '-p%s' % window, '-X', 'stuff',
                chunk_string)

def quit_screen(screen_name):
    run('screen', '-S', screen_name, '-X', 'quit')

##############################################################################
# Start Master

# This is just setting up the command that will be passed to the run method
# to start the master. The "2>%s/master.err..." line is using the stderr file
# discriptor (2) and the redirection symbol (>) to redirect any error messages
# to the master.err file in the job directory. And the "|tee %s/master.out..."
# line is piping (|) the master's output to the 'tee' command, which will split
# the output so that it writes to both the stdout (the terminal) and be saved
# in the master.out file.

master_args = [
    opts.interpreter,
    mrs_program,
    '--mrs=Master',
    '--mrs-runfile %s' % runfilename,
    '--mrs-tmpdir=%s' % opts.mrs_tmpdir,
    '--mrs-timing-file=%s' % timefilename,
    ]
if opts.mrs_profile:
    master_args.append('--mrs-profile')
if opts.mrs_verbose:
    master_args.append('--mrs-verbose')
if opts.mrs_debug:
    master_args.append('--mrs-debug')
master_args += [
    ' '.join(mrs_argv),
    '2>%s/master.err' % job_dir,
    '|tee %s/master.out' % job_dir,
    ]
master_command = ' '.join(master_args)

print('Starting the master.')

# Create a screen session named after the job name
run('screen', '-dmS', opts.jobname)

# Make the job directory (note that this will fail if it already exists).
try:
    os.makedirs(job_dir)
except OSError as e:
    errno, message = e.args
    print('Error making "%s":' % job_dir, message,
            file=sys.stderr)
    sys.exit(1)

#start the master
stuff_to_screen(opts.jobname, 0, master_command + '\n')

# Wait for the master to start and get the port number.
while True:
    try:
        with open(runfilename) as runfile:
            master_port = runfile.read().strip()
            if master_port == '-':
                print('The master exited without writing the runfile.')
                sys.exit(1)
            elif master_port:
                break
        break
    except IOError:
        pass
    time.sleep(0.05)


##############################################################################
# Start Slaves

slave_args = [
    'cd %s;' % os.getcwd(),
    opts.interpreter,
    mrs_program,
    '--mrs=Slave',
    '--mrs-master=%s:%s' % (master_hostname, master_port),
    '--mrs-tmpdir=%s' % opts.mrs_tmpdir,
    ]
if opts.mrs_profile:
    slave_args.append('--mrs-profile')
if opts.mrs_verbose:
    slave_args.append('--mrs-verbose')
if opts.mrs_debug:
    slave_args.append('--mrs-debug')

# Note that we pass ssh the -tt option to ensure that remote commands quit.
pssh_command = ' '.join((
    'pssh', host_options,
    '-o', '%s/slaves.out' % job_dir,
    '-e', '%s/slaves.err' % job_dir,
    '-t 0 -x -tt -p 1000',
    "'%s'" % ' '.join(slave_args)))

print('Starting the slaves.')

# add a second window to the screen session and run pssh command.
run('screen', '-S', opts.jobname, '-X', 'screen')
stuff_to_screen(opts.jobname, 1, pssh_command + '\n')

# Load the screen session.
if not opts.background:
    print('Loading screen session')
    run('screen', '-r', opts.jobname, '-p0')

# wait for the run file to be empty, then exit screen
print('Mrs is running, feel free to control-c to quit')
while True:
    try:
        with open(runfilename) as runfile:
            if runfile.read().strip() == '-':
                break
    except IOError:
        pass
    time.sleep(0.5)

while True:
    try:
        with open(timefilename) as timefile:
            if timefile.read().strip():
                break
    except IOError:
        pass
    time.sleep(0.1)

quit_screen(opts.jobname)

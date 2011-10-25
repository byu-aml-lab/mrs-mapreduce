#!/usr/bin/python
# Mrs
# Copyright 2008-2011 Brigham Young University
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


################################################################################
#
# This is the run-script referenced in Mrs tutorial #3. It should work as
# is, but of course is meant to be adapted as needed.
#
################################################################################

"""Runs a Mrs program using screen and pssh.

    Example:
    $ python runscript.py --hosts [slaves] [mrs program] [input]

    The 'slaves' file will be passed to the pssh program and should be a text
    file with the name of a slave machine on each line in the following format:

             [user@][host][:port]

    If the user name is left off, pssh will use the current user name, and
    for the port number, the ssh default will be used (port 22).

    Note that you will need to set up passphraseless ssh between the master
    and slave machines before running this script.

    All output is put in a folder named after the jobname. (default: 'newjob')
"""

import getpass
import optparse
import os
import socket
import subprocess
import sys
import time

DEFAULT_OUTPUT_DIR = os.getcwd() # set default to current working directory
DEFAULT_LOCAL_SCRATCH = getpass.getuser()
DEFAULT_JOBNAME = 'newjob'

##############################################################################
# Setup

# Here we use python's option parser to setup any run options we may want.
# They can be printed out with the --help option from the command line:
# example: $ python runscript.py --help
parser = optparse.OptionParser()

parser.add_option('--hosts',
        dest='hostfiles',
        action='append',
        help='Hosts file (each line "[user@][hostname][:port]")',
        default=[])
parser.add_option('--jobname',
        dest='jobname',
        action='store',
        help='Job name. Default is \'newjob\'',
        default=DEFAULT_JOBNAME)
parser.add_option('--outdir',
        dest='out_dir',
        action='store',
        help='Output directory. Default is current working directory',
        default=DEFAULT_OUTPUT_DIR)
parser.add_option('--local-scratch',
        dest='local_scratch',
        action='store',
        help='Local scratch (subdir of /net/$hostname, empty string to skip)',
        default=DEFAULT_LOCAL_SCRATCH)

opts, args = parser.parse_args()

# Make sure slave machines are specified.
if not opts.hostfiles:
    print >>sys.stderr,'No hosts file specified!'
    sys.exit(1)

# Make sure that pssh and screen are installed
rtcode1 = subprocess.call(('which', 'pssh'), stdout=open('/dev/null', 'w'))
rtcode2 = subprocess.call(('which', 'screen'), stdout=open('/dev/null', 'w'))
if rtcode1:
    print >>sys.stderr,'Error: Pssh not installed!'
    sys.exit(1)
if rtcode2:
    print >>sys.stderr,'Error: Screen not installed!'
    sys.exit(1)

# Initalize any needed variables.
mrs_program = args[0] # get name of Mrs program
mrs_argv = args[1:] # get input file
job_dir = os.path.join(opts.out_dir, opts.jobname)
local_shared = os.path.join(opts.local_scratch, opts.jobname)
runfilename = os.path.join(job_dir, 'master.run') # this will have the port num
host_options = ' '.join('--hosts %s' % hostfile for hostfile in opts.hostfiles)
master_hostname = socket.gethostname()

# Make the job directory (note that this will fail if it already exists).
try:
    os.makedirs(job_dir)
except:
    print >>sys.stderr,'Error, job directory \"%s\" may already exist!' % job_dir
    sys.exit(1)

# This method is called to run commands on the command line.
def run(*args):
    returncode = subprocess.call(args)
    if returncode != 0:
        print >>sys.stderr, 'Command failed with error code', returncode
        sys.exit(1)

##############################################################################
# Start Master

# This is just setting up the command that will be passed to the run method
# to start the master. The "2>%s/master.err..." line is using the stderr file
# discriptor (2) and the redirection symbol (>) to redirect any error messages
# to the master.err file in the job directory. And the "|tee %s/master.out..."
# line is piping (|) the master's output to the 'tee' command, which will split
# the output so that it writes to both the stdout (the terminal) and be saved
# in the master.out file.

MASTER_COMMAND = ' '.join((
    'python %s' % mrs_program, ' '.join(mrs_argv), '%s/results' % job_dir,
    '--mrs=Master',
    '--mrs-verbose',
    '--mrs-runfile %s' % runfilename,
    '--mrs-shared %s' % job_dir,
    '2>%s/master.err' % job_dir,
    '|tee %s/master.out' % job_dir))

print 'Starting the master.'

# Create a screen session named after the job name, and then start master.
run('screen', '-dmS', opts.jobname)
run('screen', '-S', opts.jobname, '-p0', '-X', 'stuff', MASTER_COMMAND + '\n')

# Wait for the master to start and get the port number.
while True:
    try:
        runfile = open(runfilename)
        break
    except IOError:
        time.sleep(0.1)

master_port = runfile.read().strip()
runfile.close()

##############################################################################
# Start Slaves

SLAVE_COMMAND = ' '.join((
    'cd %s;' % os.getcwd(),
    'python %s' % mrs_program,
    '--mrs=Slave',
    '--mrs-verbose',
    '--mrs-local-shared', ('/net/\\`hostname -s\\`/' + local_shared),
    '--mrs-master=%s:%s' % (master_hostname, master_port)))

PSSH_COMMAND = ' '.join((
    'pssh', host_options,
    '-o', '%s/slaves.out' % job_dir,
    '-e', '%s/slaves.err' % job_dir,
    '-t -1 -p 1000',
    '"%s"' % SLAVE_COMMAND))

print 'Starting the slaves.'

# add a second window to the screen session and run slave command.
run('screen', '-S', opts.jobname, '-X', 'screen')
run('screen', '-S', opts.jobname, '-p1', '-X', 'stuff', PSSH_COMMAND + '\n')

# Load the screen session.
print 'Loading screen session'
run('screen', '-r', opts.jobname, '-p0')





#!/usr/bin/python

"""Runs a Mrs program using screen and pssh.

    Example:
    $ python runscript.py --hosts slaves wordcount.py test.txt
"""

import getpass
import optparse
import os
import socket
import subprocess
import sys
import time

DEFAULT_SCRATCH_DIR = os.getcwd()
DEFAULT_LOCAL_SCRATCH = getpass.getuser()
DEFAULT_JOBNAME = 'newjob'

##############################################################################
# Setup

parser = optparse.OptionParser()

parser.add_option('--hosts', 
        dest='hostfiles', 
        action='append',
        help='Hosts file (each line "[user]@[hostname]:[port]")', 
        default=[])
parser.add_option('--jobname', 
        dest='jobname', 
        action='store',
        help='Job name. Default is \'newjob\'', 
        default=DEFAULT_JOBNAME)
parser.add_option('--scratch', 
        dest='scratch_dir', 
        action='store',
        help='Scratch directory. Default is current working directory', 
        default=DEFAULT_SCRATCH_DIR)
parser.add_option('--local-scratch', 
        dest='local_scratch', 
        action='store',
        help='Local scratch (subdir of /net/$hostname, empty string to skip)',
        default=DEFAULT_LOCAL_SCRATCH)
        
opts, args = parser.parse_args()

if not opts.hostfiles:
    print >>sys.stderr,'No hosts file specified!'
    sys.exit(1)

# set some variables
mrs_program = args[0]
mrs_argv = args[1:]
job_dir = os.path.join(opts.scratch_dir, opts.jobname)
local_shared = os.path.join(opts.local_scratch, opts.jobname)
runfilename = os.path.join(job_dir, 'master.run')
host_options = ' '.join('--hosts %s' % hostfile for hostfile in opts.hostfiles)
master_hostname = socket.gethostname()

# This method is called to run commands on the command line
def run(*args):
    returncode = subprocess.call(args)
    if returncode != 0:
        print >>sys.stderr, 'Command failed with error code', returncode
        sys.exit(1)

##############################################################################
# Start Master

MASTER_COMMAND = ' '.join((
    'python %s' % mrs_program, ' '.join(mrs_argv), '%s/results' % job_dir,
    '--mrs=Master',
    '--mrs-verbose',
    '--mrs-runfile %s' % runfilename,
    '--mrs-shared %s' % job_dir,
    '2>%s/master.err' % job_dir,
    '|tee %s/master.out' % job_dir))

# Make the job directory (note that this will fail if it already exists).
try:
    os.makedirs(job_dir)
except:
    print >>sys.stderr,'Error, job directory \"%s\" may already exist!' % job_dir
    sys.exit(1)

# Create a screen session named after the job name.
run('screen', '-dmS', opts.jobname)
run('screen', '-S', opts.jobname, '-X', 'screen')

print 'Starting the master.'
run('screen', '-S', opts.jobname, '-p0', '-X', 'stuff', MASTER_COMMAND + '\n')

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
run('screen', '-S', opts.jobname, '-p1', '-X', 'stuff', PSSH_COMMAND + '\n')

# Load the screen session.
print 'Loading screen session'
run('screen', '-r', opts.jobname, '-p0')



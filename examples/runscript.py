#!/usr/bin/python

"""Runs a Mrs program using screen and pssh.

    Example:
    $ python runscript.py --hosts slaves wordcount.py mytxt.txt
    
    The 'slaves' file will be passed to the pssh program and should be a text
    file with the name of a slave machine on each line in the following format:
            
             [user@][host][:port]
             
    If the user name is left off, pssh will use the current user name, and
    likewise for the port number the ssh default will be used (port 22).
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

# Here we use python's option parser to setup any run options we may want.
# They can be prented out with the --help option from the command line:
# example: $ python runscript.py --help
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

# Make sure slave machines are specified.
if not opts.hostfiles:
    print >>sys.stderr,'No hosts file specified!'
    sys.exit(1)

# Initalize any needed variables.
mrs_program = args[0]
mrs_argv = args[1:]
job_dir = os.path.join(opts.scratch_dir, opts.jobname)
local_shared = os.path.join(opts.local_scratch, opts.jobname)
runfilename = os.path.join(job_dir, 'master.run')
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

MASTER_COMMAND = ' '.join((
    'python %s' % mrs_program, ' '.join(mrs_argv), '%s/results' % job_dir,
    '--mrs=Master',
    '--mrs-verbose',
    '--mrs-runfile %s' % runfilename,
    '--mrs-shared %s' % job_dir,
    '2>%s/master.err' % job_dir,
    '|tee %s/master.out' % job_dir))

print 'Starting the master.'

# Create a screen session named after the job name and start master.
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





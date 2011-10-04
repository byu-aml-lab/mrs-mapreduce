#!/usr/bin/python

"""Runs a Mrs program using screen and pssh.

    Example:
    $ python potato.py --hosts ~/admin/potatoes/russet --jobname wordcount wordcount.py bom.txt out
"""

import getpass
import optparse
import os
import socket
import subprocess
import sys
import time

DEFAULT_SCRATCH_DIR = '/'.join(('/aml/scratch', getpass.getuser()))
DEFAULT_LOCAL_SCRATCH = getpass.getuser()
DEFAULT_JOBNAME = 'newjob'

##############################################################################
# Setup

parser = optparse.OptionParser(conflict_handler='resolve')
parser.disable_interspersed_args()

parser.add_option('--hosts', 
        dest='hostfiles', 
        action='append',
        help='hosts file (each line "[user@]host[:port]")', 
        default=[])
parser.add_option('--jobname', 
        dest='jobname', 
        action='store',
        help='job name', 
        default=DEFAULT_JOBNAME)
parser.add_option('--scratch', 
        dest='scratch_dir', 
        action='store',
        help='scratch directory', 
        default=DEFAULT_SCRATCH_DIR)
parser.add_option('--local-scratch', 
        dest='local_scratch', 
        action='store',
        help='local scratch (subdir of /net/$hostname, empty string to skip)',
        default=DEFAULT_LOCAL_SCRATCH)
        
opts, args = parser.parse_args()

mrs_program = args[0]
mrs_argv = args[1:]

if not opts.hostfiles:
    print >>sys.stderr,'No hosts file specified!'
    sys.exit(1)

if opts.jobname:
    outdir = os.path.join(opts.scratch_dir, opts.jobname)
    local_shared = os.path.join(opts.local_scratch, opts.jobname)
else:
    outdir = os.path.join(opts.scratch_dir, mrs_program)
    local_shared = os.path.join(opts.local_scratch, mrs_program)
    
runfilename = os.path.join(outdir, 'master.run')

# This method is called to run commands on the command line
def run(*args):
    returncode = subprocess.call(args)
    if returncode != 0:
        print >>sys.stderr, 'Command failed with error code', returncode
        sys.exit(1)

##############################################################################
# Start Master

MASTER_COMMAND = ' '.join((
    'python %s' % mrs_program,
    ' '.join(mrs_argv),
    '--mrs=Master',
    '--mrs-verbose',
    '--mrs-runfile %s' % runfilename,
    '--mrs-shared %s' % outdir,
    '2>%s/master.err' % outdir,
    '|tee %s/master.out' % outdir))

STDERR_COMMAND = 'tail -F %s/master.err' % outdir

# Make the job directory (note that this will fail if it already exists).
try:
    os.makedirs(outdir)
except:
    print >>sys.stderr,'Error, job directory \"%s\" may already exist!' % outdir
    sys.exit(1)

# Create a screen session named after the job name.
run('screen', '-dmS', opts.jobname)

# Add a second and third window in the screen session.
run('screen', '-S', opts.jobname, '-X', 'screen')
run('screen', '-S', opts.jobname, '-X', 'screen')

print 'Starting the master.'
run('screen', '-S', opts.jobname, '-p0', '-X', 'stuff', MASTER_COMMAND + '\n')

print 'Waiting for the master to start up.'
while True:
    try:
        runfile = open(runfilename)
        break
    except IOError:
        time.sleep(0.1)

master_port = runfile.read().strip()
runfile.close()

run('screen', '-S', opts.jobname, '-p1', '-X', 'stuff', STDERR_COMMAND + '\n')

##############################################################################
# Start Slaves

host_options = ' '.join('-h %s' % hostfile for hostfile in opts.hostfiles)
pwd = os.getcwd()
master_hostname = socket.gethostname()

SLAVE_COMMAND = ' '.join((
    'cd %s;' % pwd,
    'python %s' % mrs_program,
    '--mrs=Slave',
    '--mrs-verbose',
    '--mrs-local-shared', ('/net/\\`hostname -s\\`/' + local_shared),
    '--mrs-master=%s:%s' % (master_hostname, master_port)))
    
PSSH_COMMAND = ' '.join((
    'pssh', host_options,
    '-o', '%s/slaves.out' % outdir, 
    '-e', '%s/slaves.err' % outdir,
    '-t -1 -p 1000',
    '"%s"' % SLAVE_COMMAND))

print 'Starting the slaves.'
run('screen', '-S', opts.jobname, '-p2', '-X', 'stuff', PSSH_COMMAND + '\n')

# Load the screen session.
print 'Connecting.'
run('screen', '-r', opts.jobname, '-p0')



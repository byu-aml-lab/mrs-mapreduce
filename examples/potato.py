#!/usr/bin/python

"""Runs a Mrs program using screen and pssh."""

import getpass, os, socket, subprocess, sys, time

DEFAULT_SCRATCH_DIR = '/'.join(('/aml/scratch', getpass.getuser()))
PYTHON = 'python'
HOSTFILES = ['/admin/potatoes/all4']


##############################################################################
# Setup

DEFAULT_JOBNAME = 'mrspotato'
JOBNAME = os.getenv('JOBNAME')
if not JOBNAME:
    JOBNAME = DEFAULT_JOBNAME

SCRATCH_DIR = os.getenv('SCRATCH_DIR')
if not SCRATCH_DIR:
    SCRATCH_DIR = DEFAULT_SCRATCH_DIR

OUTDIR = os.path.join(SCRATCH_DIR, JOBNAME)
RUNFILE = os.path.join(OUTDIR, 'master.run')

mrs_program = sys.argv[1]
mrs_argv = sys.argv[2:]

def run(*args):
    returncode = subprocess.call(args)
    if returncode != 0:
        print >>sys.stderr, 'Command failed with error code', returncode
        sys.exit(1)

##############################################################################
# Master

MASTER_COMMAND = ' '.join((PYTHON, mrs_program,
    ' '.join(mrs_argv),
    '-I Master --mrs-debug',
    '--mrs-runfile', RUNFILE,
    '--mrs-shared', OUTDIR,
    '2>%s/master.err' % OUTDIR,
    '|tee %s/master.out' % OUTDIR))

STDERR_COMMAND = 'tail -F %s/master.err' % OUTDIR


# Make the job directory (note that this will fail if it already exists).
os.mkdir(OUTDIR)

# Create a screen session named after the JOBNAME.
run('screen', '-dmS', JOBNAME)
# Add a second and third window in the screen session.
run('screen', '-S', JOBNAME, '-X', 'screen')
run('screen', '-S', JOBNAME, '-X', 'screen')

print 'Starting the master.'
# Paste the commands into separate windows (use "^a n" and "^a p" to switch).
run('screen', '-S', JOBNAME, '-p0', '-X', 'stuff', MASTER_COMMAND + '\n')
print 'Waiting for the master to start up.'

while True:
    try:
        runfile = open(RUNFILE)
        break
    except IOError:
        time.sleep(0.1)

master_port = runfile.read().strip()
runfile.close()

run('screen', '-S', JOBNAME, '-p1', '-X', 'stuff', STDERR_COMMAND + '\n')


##############################################################################
# Slave

host_options = ' '.join('-h %s' % hostfile for hostfile in HOSTFILES)
pwd = os.getcwd()
hostname = socket.gethostname()
SLAVE_COMMAND = ' '.join(('cd %s;' % pwd,
    PYTHON, mrs_program,
    '-I Slave --mrs-verbose',
    '-M', '%s:%s' % (hostname, master_port)))
PSSH_COMMAND = ' '.join(('pssh', host_options,
    '-o', '%s/slaves.out' % OUTDIR, '-e', '%s/slaves.err' % OUTDIR,
    '-t -1 -p 1000',
    '"%s"' % SLAVE_COMMAND))

print 'Starting the slaves.'
run('screen', '-S', JOBNAME, '-p2', '-X', 'stuff', PSSH_COMMAND + '\n')

# Load the screen session.
print 'Connecting.'
run('screen', '-r', JOBNAME, '-p0')

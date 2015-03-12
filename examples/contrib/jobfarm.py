#!/usr/bin/python
#####################################################
# This script simply farms out a list of 
# commands to various mrs slaves. It is a 
# trivial (ab)use of the power of map-reduce
# to solve the problem of distributing many 
# jobs to many machines. 
#
# To customize this script, search for the TODO 
# items below and follow the instructions.
#
# 0)
# Set up public/private ssh keys on your master and 
# cluster machines. (Hint: if your home directory is shared 
# via NFS with the cluster, then this is one step).
#
# 1)
# Run this mrs job on a cluster by doing, for example:
# ./clusterrun.py -h slaves -o out -n myjobname jobfarm.py --myopt=9
#
# 2) 
# A 'screen' session will be opened with your job running inside
# See program results in out/myjobname/slaves.out/
#
# 3) Customize your cluster by editing the 'slaves' file.
# If you include the same machine on two separate lines, 
# then two processes will be run on that machine 
# concurrently
#####################################################

import mrs
import itertools
import os
import subprocess

#####################################################
# TODO: replace this job generator with your own 
# generator that yields the list of jobs you'd like 
# farmed out onto your cluster.
# The "jobs" being returned are simply strings
# that can be exectued on the shell.
#####################################################
def job_generator(opts):
    for i in range(1000):
        yield "echo hi #%d with parameter %s" % (i,opts.myopt)
    

# splits says how many pieces the job list should be broken 
# into before being passed around to the machines. It 
# should be set to something reasonable like 3-5 x number_of_machines
jobfarmsplits = 300

class JobLauncher(mrs.GeneratorCallbackMR):
    """Uses mrs to schedule arbitrary command-line jobs on host machines"""

    def __init__(self, opts, args):
        super(JobLauncher, self).__init__(opts, args)
        self.opts = opts


    '''
    This method is called every time the main program loop yields. 
    This happens each time jobs are farmed out, finished, and results
    are aggregated. When it returns False, the program ends. 

    This program only farms jobs out once and then ends.
    '''
    def callback(self,ds):
        print "------FINISHED-----"
        return False

    '''
    This method is called once at the beginning of the job, and is in charge
    of iteratively mapping and reducing. It can take a break as often as
    it chooses by yielding a dataset representing the current state 
    of computation, along with a callback method that is in charge 
    of determining whether or not the program is finished, based 
    on the state of the dataset.
    '''
    def generator(self, job):
        # bogus dataset. We will run only 1 iteration
        # and the map function will do all the work
        # of generating jobs
        ds = job.local_data([(1,["bogus"])],splits=jobfarmsplits)

        while True:
            ds = job.map_data(ds,mapper=self.map,splits=jobfarmsplits) 
            ds = job.reduce_data(ds,self.reduce,splits=jobfarmsplits)
            yield (ds, self.callback)

    '''
    inputs
        key: the key value previously associated with these values. 
        values: values that need to be assigned new keys
    outputs
        yield a number of key value pairs
    '''
    def map(self, key, values):
        """ enumerate the jobs, ignoring old key/values """
        print "generating jobs . . ."
        i = 0
        for job in job_generator(self.opts):
            if job is not None:
                i+=1
                yield (i, job)

    '''
    inputs
        key: the index of the job
        jobs: the list of jobs to be run on this machine
    '''
    def reduce(self, key, jobs):
        """ run the job """
        for job in jobs:
            # execute the job string
            print "\nRunning cmd %d\n\t%s" % (key,job)
            os.system(job)

        print "============= Done! ==================="
        yield ["Done"]


    '''
    Define a set of command line parameters whose values will be 
    passed to your program in the opts parameter of the __init__ method.
    '''
    @classmethod
    def update_parser(cls, parser):
        # TODO: add your option(s) here
        parser.add_option('--myopt',
                type='int',
                dest='myopt',
                default=1,
                help='Myopt determines blah blah blah...',
                )
        return parser

if __name__ == '__main__':
    mrs.main(JobLauncher)

# vim: et sw=4 sts=4

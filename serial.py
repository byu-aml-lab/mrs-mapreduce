# Mrs
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

def serial_main(registry, user_run, user_setup, args, opts):
    """Mrs Serial.
    """
    from job import Job

    job = Job(registry, user_run, user_setup, args, opts)
    serial = Serial(job)
    job.start()
    serial.run()


def mockparallel_main(registry, user_run, user_setup, args, opts):
    """Run Mrs Mockparallel.

    This creates all of the tasks that are used in the normal parallel
    implementation, but it executes them in serial.  This can make debugging a
    little easier.
    """

    raise NotImplementedError("The mockparallel implementation is temporarily"
            " broken.  Sorry.")

    from job import Job
    from util import try_makedirs

    # Set up shared directory
    try_makedirs(opts.mrs_shared)

    job = Job(registry, user_run, user_setup, args, opts)
    mockparallel(job)


class Serial(object):
    """Runs a MapReduce job in serial."""
    def __init__(self, job):
        self.job = job
        import threading
        self.cv = threading.Condition()
        job.update_callback = job.end_callback = self.job_updated

    def run(self):
        while self.ready():
            dataset = self.job.active_data[0]
            dataset.run_serial()
            self.job.check_done()

    def ready(self):
        """Waits for a dataset to become ready.

        Returns True when a dataset is ready and False when the job is
        finished.
        """
        # A quick optimization for jobs with lots of datasets:
        if self.job.active_data:
            return True

        self.cv.acquire()
        try:
            while True:
                if self.job.active_data:
                    return True
                elif self.job.done():
                    return False
                else:
                    self.cv.wait()
        finally:
            self.cv.release()

    def job_updated(self):
        """Called when the job is updated or completed.
        
        Called from another thread.
        """
        self.cv.acquire()
        self.cv.notify()
        self.cv.release()


def mockparallel(job):
    """MapReduce execution on POSIX shared storage, such as NFS.
    
    Note that progress times often seem wrong in mockparallel.  The reason is
    that most of the execution time is in I/O, and mockparallel tries to load
    the input for all reduce tasks before doing the first reduce task.
    """
    job.start()

    while not job.done():
        task = job.schedule()
        # FIXME (busy loop):
        if task is None:
            continue
        task.active()
        task.run()
        task.finished()
        job.check_done()

    job.join()

# vim: et sw=4 sts=4

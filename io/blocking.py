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

"""Blocking IO.

Certain IO operations cannot be performed in a non-blocking manner, so they
must not be performed within the Twisted reactor.  Ordinary files are
particularly troublesome.  True asynchronous IO (libaio) isn't well supported
in Linux yet (where it is often implemented by blocking IO in threads), much
less in Windows.  The select and poll syscalls are useless because they
_always_ report ordinary files as ready for reads or writes, even if those
operations will in fact block.  The reason for this is that files permit
random access, so select and poll have no way to tell where the next read or
write will take place.  A hypothetical "streaming read file" API or a pipe
connected directly to a file would solve this problem, but neither exist.
Anyway, until cross-platform asynchronous IO is widespread (ha!), doing reads
in a separate thread is the only way to go.  The thread might as well also use
network protocols that aren't supported by Twisted.
"""

import threading


def read():
    import urlparse
    parsed_url = urlparse.urlsplit(url, 'file')
    if parsed_url.scheme == 'file':
        f = open(parsed_url.path)
        buf = Buffer(filelike=f)
    else:
        from net import download
        buf = download(url)
    return buf


class BlockingThread(threading.Thread):
    """Loads files and urls with blocking reads.
    
    This thread deals with IO that cannot be handled natively in Twisted.  A
    task or producer is an object which has a `run` method.  Note that the
    BlockingThread starts lazily (once something has been registered).
    """
    def __init__(self, *args, **kwds):
        threading.Thread.__init__(self, *args, **kwds)
        self.setName('BlockingThread')
        # Set this thread to die when the main thread quits.
        self.setDaemon(True)
        self.started = False
        self.started_lock = threading.Lock()

        import Queue
        self.queue = Queue.Queue()

    def register(self, task):
        """Registers a blocking producer or task.
        
        Called from other threads.
        """
        self.queue.put(task)
        if not self.started:
            self.started_lock.acquire()
            try:
                if not self.started:
                    self.start()
                    self.started = True
            finally:
                self.started_lock.release()

    def run(self):
        while True:
            task = self.queue.get()
            task.run()


class RecursiveRemover(object):
    """A task that recursively removes a tree.

    Since this can block, it needs to run within BlockingThread.
    """
    def __init__(self, path):
        self.path = path

    def run(self):
        from util import remove_recursive
        remove_recursive(self.path)

# vim: et sw=4 sts=4

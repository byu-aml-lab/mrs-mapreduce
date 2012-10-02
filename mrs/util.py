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

"""Miscellaneous Helper Functions"""

from __future__ import division, print_function

import errno
import math
import os
import random
import select
import string
import subprocess
import sys
import tempfile
import time

from logging import getLogger
logger = getLogger('mrs')

PROFILE_DIR = './mrsprof'
TEMPFILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
ID_CHARACTERS = string.ascii_letters + string.digits
BITS_IN_DOUBLE = 53
ID_MAXLEN = int(BITS_IN_DOUBLE * math.log(2) / math.log(len(ID_CHARACTERS)))
ID_RANGES = [len(ID_CHARACTERS) ** i for i in range(ID_MAXLEN + 1)]

# Python 3 compatibility
PY3 = sys.version_info[0] == 3
if not PY3:
    range = xrange


class EventLoop(object):
    """A very simple event loop that wraps select.poll.

    As far as event loops go, this is pretty lame.  Since the multiprocessing
    module's send/recv methods don't support partial reads, there will still
    be a significant amount of blocking.  Likewise, this simplistic loop does
    not support POLLOUT.  If it becomes necessary, it won't be too hard to
    write a simple replacement for multiprocessing's Pipe that supports
    partial reading and writing.

    Attributes:
        handler_map: map from file descriptors to methods for handling reads
        poll: poll object (from the select module)
        running: bool indicating whether the event loop should continue
    """
    def __init__(self):
        self.handler_map = {}
        self.running = True
        self.poll = select.poll()

    def register_fd(self, fd, handler):
        """Registers the given file descriptor and handler with poll.

        Assumes that the file descriptors are only used in read mode.
        """
        self.handler_map[fd] = handler
        self.poll.register(fd, select.POLLIN)

    def run(self, timeout_function=None):
        """Repeatedly calls poll to read from various file descriptors.

        The timeout_function is called each time through the loop, and its
        value is used as the timeout for poll (None means to wait
        indefinitely).
        """
        while self.running:
            # Note that poll() is unaffected by siginterrupt/SA_RESTART (man
            # signal(7) for more detail), so we check explicitly for EINTR.
            try:
                if timeout_function:
                    timeout = timeout_function()
                else:
                    timeout = None
                for fd, event in self.poll.poll(timeout):
                    self.handler_map[fd]()
            except select.error as e:
                if e.args[0] != errno.EINTR:
                    raise


def try_makedirs(path):
    """Do the equivalent of mkdir -p."""
    # Workaround for Python issue #14702:
    try:
        os.stat(path)
    except OSError:
        pass
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

def remove_recursive(path):
    """Do the equivalent of rm -r."""
    p = subprocess.Popen(['/bin/rm', '-rf', path])
    retcode = p.wait()
    if retcode == 0:
        return
    else:
        message = 'Failed to delete some of %s (probably due to NFS).' % path
        logger.warning(message)

def delta_seconds(delta):
    """Find the total number of seconds in a timedelta object.

    Flatten out the days and microseconds to get a simple number of seconds.
    """
    day_seconds = 24 * 3600 * delta.days
    ms_seconds = delta.microseconds / 1000000.0
    total = day_seconds + delta.seconds + ms_seconds
    return total

def random_string(length):
    """Returns a string of the given (short) length suitable for a random ID."""
    # Note that we do this by hand instead of calling random.choice because
    # it's much, much faster (and we call it a lot).
    choices = len(ID_CHARACTERS)
    try:
        r = int(random.random() * ID_RANGES[length])
    except IndexError:
        raise RuntimeError('Cannot create a string of length %s' % length)

    s = ''
    for place in range(length):
        index = int(r % choices)
        s += ID_CHARACTERS[index]
        r /= choices
    return s

def mktempfile(dir, prefix, suffix):
    """Creates and opens a new temporary file with a unique filename.

    Returns a (file object, path) pair.  The file is opened in binary write
    mode.  This falls back to tempfile.NamedTemporaryFile if necessary, but by
    default it uses the faster strategy of not adding random characters to the
    filename.  Note that to save time, we don't set O_CLOEXEC, which is not
    necessary in Mrs.
    """
    path = dir + '/' + prefix + suffix
    try:
        fd = os.open(path, TEMPFILE_FLAGS, 0o600)
        f = os.fdopen(fd, 'wb')
    except OSError:
        f = tempfile.NamedTemporaryFile(delete=False, dir=dir,
                prefix=prefix, suffix=suffix)
        path = f.name
    return f, path

def mktempdir(dir, prefix):
    for i in range(tempfile.TMP_MAX):
        name = dir + '/' + prefix + random_string(6)
        try:
            os.mkdir(name, 0o700)
            return name
        except OSError:
            pass

def _call_under_profiler(function, args, kwds, prof):
    """Calls a function with arguments under the given profiler.

    Returns the return value of the function, or None if it is unavailable.
    """
    returnvalue = []
    def f():
        value = function(*args, **kwds)
        returnvalue.append(value)

    prof.runctx('f()', locals(), globals())
    return returnvalue[0]

def profile_loop(function, args, kwds, filename, min_delay=5):
    """Repeatedly runs a function (with args) and collects cumulative stats.

    Runs as long as the function returns True.  The min_delay parameter
    determines the minimum delay, in seconds, between dumps to the stats file.
    """
    import cProfile
    prof = cProfile.Profile()

    try_makedirs(PROFILE_DIR)
    tmp_path = '%s/.%s' % (PROFILE_DIR, filename)
    path = '%s/%s' % (PROFILE_DIR, filename)

    try:
        os.remove(path)
    except OSError:
        pass

    keep_going = True
    last_time = time.time()
    while keep_going:
        try:
            keep_going = _call_under_profiler(function, args, kwds, prof)
        finally:
            now = time.time()
            if (now - last_time > min_delay) or not keep_going:
                last_time = now
                prof.dump_stats(tmp_path)
                os.rename(tmp_path, path)

def profile_call(function, args, kwds, filename):
    """Profiles a function with args, outputing stats to a file.

    Returns the return value of the function, or None if it is unavailable.
    """
    import cProfile
    prof = cProfile.Profile()

    try_makedirs(PROFILE_DIR)
    tmp_path = '%s/.%s' % (PROFILE_DIR, filename)
    path = '%s/%s' % (PROFILE_DIR, filename)

    try:
        os.remove(path)
    except OSError:
        pass

    try:
        return _call_under_profiler(function, args, kwds, prof)
    finally:
        prof.dump_stats(tmp_path)
        os.rename(tmp_path, path)

# vim: et sw=4 sts=4

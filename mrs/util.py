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

"""Miscellaneous Helper Functions"""

from __future__ import division

import math
import os
import random
from six.moves import xrange as range
import string
import subprocess
import tempfile
import time

from logging import getLogger
logger = getLogger('mrs')

TEMPFILE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
ID_CHARACTERS = string.ascii_letters + string.digits
BITS_IN_DOUBLE = 53
ID_MAXLEN = int(BITS_IN_DOUBLE * math.log(2) / math.log(len(ID_CHARACTERS)))
ID_RANGES = [len(ID_CHARACTERS) ** i for i in range(ID_MAXLEN + 1)]


def try_makedirs(path):
    """Do the equivalent of mkdir -p."""
    try:
        os.makedirs(path)
    except OSError as e:
        import errno
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
        fd = os.open(path, TEMPFILE_FLAGS, 0600)
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
            os.mkdir(name, 0700)
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
    tmp_filename = '.' + filename

    try:
        os.remove(filename)
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
                prof.dump_stats(tmp_filename)
                os.rename(tmp_filename, filename)

def profile_call(function, args, kwds, filename):
    """Profiles a function with args, outputing stats to a file.

    Returns the return value of the function, or None if it is unavailable.
    """
    import cProfile
    prof = cProfile.Profile()
    tmp_filename = '.' + filename

    try:
        os.remove(filename)
    except OSError:
        pass

    try:
        return _call_under_profiler(function, args, kwds, prof)
    finally:
        prof.dump_stats(tmp_filename)
        os.rename(tmp_filename, filename)

# vim: et sw=4 sts=4

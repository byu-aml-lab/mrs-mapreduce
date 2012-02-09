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

import os
import random
import string
import subprocess

from logging import getLogger
logger = getLogger('mrs')


def try_makedirs(path):
    """Do the equivalent of mkdir -p."""
    try:
        os.makedirs(path)
    except OSError, e:
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
    possible = string.ascii_letters + string.digits
    return ''.join(random.choice(possible) for i in xrange(length))

# vim: et sw=4 sts=4

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


def mrs_simple(job, args, opts):
    """Default run function for a map phase and reduce phase"""
    from io.textformat import TextWriter

    if len(args) < 2:
        import sys
        print >>sys.stderr, "Requires input(s) and an output."
        return

    source = job.file_data(args[:-1])
    intermediate = job.map_data(source, 'mapper')
    output = job.reduce_data(intermediate, 'reducer', outdir=args[-1],
            format=TextWriter)
    job.end()

    ready = []
    while not ready:
        ready = job.wait(output, timeout=2.0)
        print job.status()


# vim: et sw=4 sts=4

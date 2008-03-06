# Mrs
# Copyright 2008 Andrew McNabb <amcnabb-mrs@mcnabbs.org>
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option)
# any later version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for
# more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Mrs.  If not, see <http://www.gnu.org/licenses/>.


def mrs_simple(job, args, opts):
    """Default run function for a map phase and reduce phase"""
    from io import TextWriter

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

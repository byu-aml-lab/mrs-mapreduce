#!/usr/bin/env python

# Copyright 2008 Brigham Young University
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
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

def mapper(key, value):
    for word in value.split():
        yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

def wordcount_serial():
    from mrs.mapreduce import Operation, SerialJob
    import sys
    if len(sys.argv) != 2:
        print "usage: %s inputfile" % sys.argv[0]
        sys.exit(-1)
    filename = sys.argv[1]
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(output_dir='.')
    mrsjob.inputs.append(filename)
    mrsjob.operations.append(op)
    mrsjob.run()

def wordcount_posix():
    from mrs.mapreduce import Operation, POSIXJob
    import sys
    if len(sys.argv) < 2:
        print "usage: %s inputfile1 [inputfile2 ...]" % sys.argv[0]
        sys.exit(-1)
    filenames = sys.argv[1:]
    op = Operation(mapper, reducer, map_tasks=len(filenames),
            reduce_tasks=2)
    mrsjob = POSIXJob('/home/amcnabb/python/mrs/tmp')
    mrsjob.inputs = filenames
    mrsjob.operations.append(op)
    mrsjob.run()

if __name__ == '__main__':
    #wordcount_serial()
    wordcount_posix()

# vim: et sw=4 sts=4

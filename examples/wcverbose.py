#!/usr/bin/python
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

from string import punctuation
import mrs

def mapper(key, value):
    for word in value.split():
        word = word.strip(punctuation).lower()
        if word:
            yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

def run(job, args, opts):
    source = job.file_data(args[:-1])
    intermediate = job.map_data(source, mapper)
    output = job.reduce_data(intermediate, reducer,
            outdir=args[-1], format=mrs.TextWriter)
    job.end()

    ready = []
    while not ready:
        ready = job.wait(output, timeout=2.0)
        job.print_status()

if __name__ == '__main__':
    mrs.main(mrs.Registry(globals()), run)

# vim: et sw=4 sts=4

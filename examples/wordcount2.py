#!/usr/bin/python
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

################################################################################
#
# wordcount2.py is just like wordcount.py except that it allows you to pass in
# a single file that enumerates a set of input text files. (See the examples
# folder for an example input file.) This is done by overriding the input_data()
# function in MRS_HOME/mrs/mapreduce.py.
#
################################################################################

import string
import mrs

class WordCount(mrs.MapReduce):

    def map(self, key, value):
        for word in value.split():
            word = word.strip(string.punctuation).lower()
            if word.isalpha():
                yield (word, str(1))

    def reduce(self, key, values):
        yield str(sum(int(x) for x in values))

    def input_data(self, job):
        if len(self.args) < 2:
            print >>sys.stderr, "Requires input(s) and an output."
            return None
        inputs = []
        f = open(self.args[0])
        for line in f:
            inputs.append(line[:-1])
        return job.file_data(inputs)

if __name__ == '__main__':
    mrs.main(WordCount)

# vim: et sw=4 sts=4

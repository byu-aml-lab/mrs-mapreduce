#!/usr/bin/python
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

from __future__ import print_function

import string
import mrs

class WordCount2(mrs.MapReduce):
    """Count the number of occurrences of each word in a set of documents.

    This program is just like wordcount.py except:

    1. It takes a single file that enumerates a set of input text files.  This
    is done by overriding the input_data() function in
    MRS_HOME/mrs/mapreduce.py.

    2. It is smart about punctuation, case, etc.

    3. It uses the reduce function as a combiner.

    4. It uses custom serializers instead of pickle.
    """

    @mrs.key_serializer('str')
    @mrs.value_serializer('int')
    def map(self, key, value):
        for word in value.split():
            word = word.strip(string.punctuation).lower()
            if word:
                yield (word, 1)

    def reduce(self, key, values):
        yield sum(values)

    combine = reduce

    def input_data(self, job):
        if len(self.args) < 2:
            print("Requires input(s) and an output.", file=sys.stderr)
            return None
        inputs = []
        for filename in self.args[:-1]:
            with open(filename) as f:
                for line in f:
                    inputs.append(line.strip())
        return job.file_data(inputs)

if __name__ == '__main__':
    mrs.main(WordCount2)

# vim: et sw=4 sts=4

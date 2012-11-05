#!/usr/bin/python
# Mrs
# Copyright 2008-2012 Brigham Young University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

    @mrs.output_serializers(key=mrs.MapReduce.str_serializer,
                            value=mrs.MapReduce.int_serializer)
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

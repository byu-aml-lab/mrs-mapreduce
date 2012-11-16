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

import mrs
import string

class WordCount(mrs.MapReduce):
    """Count the number of occurrences of each word in a set of documents.

    Word Count is the classic "hello world" MapReduce program. This is a
    working example and is provided to demonstrate a simple Mrs program. It is
    further explained in the tutorials provided in the docs directory.
    """
    def map(self, line_num, line_text):
        for word in line_text.split():
            word = word.strip(string.punctuation).lower()
            if word:
                yield (word, 1)

    def reduce(self, word, counts):
        yield sum(counts)

if __name__ == '__main__':
    mrs.main(WordCount)

# vim: et sw=4 sts=4

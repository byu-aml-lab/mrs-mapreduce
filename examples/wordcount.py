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

import mrs
import string

class WordCount(mrs.MapReduce):
    """Count the number of occurrences of each word in a set of documents.

    Word Count is the classic "hello world" MapReduce program. This is a
    working example and is provided to demonstrate a simple Mrs program. It is
    further explained in the tutorials provided in the docs directory.
    """
    def map(self, key, value):
        for word in value.split():
            word = word.strip(string.punctuation).lower()
            if word:
                yield (word, 1)

    def reduce(self, key, values):
        yield sum(values)

if __name__ == '__main__':
    mrs.main(WordCount)

# vim: et sw=4 sts=4

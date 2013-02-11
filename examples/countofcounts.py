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

class IterativeWordCount(mrs.GeneratorCallbackMR):
    """A hello world iterative mapreduce program. 

    The first iteration yields a classic word count.
    The second iteration yields a count of counts (key=1,value=number of words that occured once).
    The third iteration yields a count of count of counts, and so forth.
    """

    def callback(self,ds):
        data_size = len([d for d in ds.data()])
        print("finished iteration. %d items counted."%data_size)
        return data_size>1

    def generator(self, job):
        itr = 0
        # read in data (list of file supplied by the CLI)
        # input_data is defined on the MapReduce class
        ds = self.input_data(job) # key=file, val=line
        # do the first map reduce manually (count words)
        ds = job.map_data(ds,mapper=self.map_words) # key=word, val=int
        ds = job.reduce_data(ds,self.reduce,outdir="%s/%d"%(self.args[-1],itr),format=mrs.fileformats.TextWriter)
        yield (ds,self.callback)

        itr = itr + 1
        while True:
            # iteratively map reduce (count counts)
            ds = job.map_data(ds,mapper=self.map_counts) # key=int, val=int
            ds = job.reduce_data(ds,self.reduce,outdir="%s/counts_of_counts%d"%(self.args[-1],itr),format=mrs.fileformats.TextWriter)
            itr = itr + 1
            yield (ds, self.callback)

    # count the counts
    def map_counts(self, key, count):
        yield (count, 1)

    # count the words
    def map_words(self, line_num, line_text):
        for word in line_text.split():
            word = word.strip(string.punctuation).lower()
            if word:
                yield (word, 1)

    # aggregate the counts
    def reduce(self, key, counts):
        yield sum(counts)


if __name__ == '__main__':
    mrs.main(IterativeWordCount)

# vim: et sw=4 sts=4

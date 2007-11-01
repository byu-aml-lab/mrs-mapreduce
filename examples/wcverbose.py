#!/usr/bin/env python
from string import punctuation
import mrs

def mapper(key, value):
    for word in value.split():
        word = word.strip(punctuation).lower()
        if word:
            yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

# options to be passed in (?):
#  - default # of map tasks
#  - default # of reduce tasks
#  - input
#  - output
def program(input):
    # TODO: introduce splitting mechanisms (i.e. mrs.Output(mrs.Map()))
    data1 = mrs.DataSet(mapper, input)
    output = mrs.DataSet(reducer, data1.partition())
    return output

if __name__ == '__main__':
    mrs.main(program)

# vim: et sw=4 sts=4

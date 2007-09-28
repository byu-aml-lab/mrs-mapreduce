#!/usr/bin/env python

from mrs.mapreduce import mapreduce

def mapper(key, value):
    for word in value.split():
        yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

mapreduce(mapper, reducer, 'testinput.txt')

# vim: et sw=4 sts=4

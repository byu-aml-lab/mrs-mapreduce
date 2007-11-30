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

if __name__ == '__main__':
    mrs.main(mrs.Registry(globals()))

# vim: et sw=4 sts=4

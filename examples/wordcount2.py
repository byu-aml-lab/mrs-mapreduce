#!/usr/bin/env python
from string import punctuation

def mapper(key, value):
    for word in value.split():
        word = word.strip(punctuation).lower()
        if word:
            yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

if __name__ == '__main__':
    import mrs
    mrs.main(mapper, reducer)

# vim: et sw=4 sts=4

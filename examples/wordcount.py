#!/usr/bin/env python

def mapper(key, value):
    for word in value.split():
        yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

if __name__ == '__main__':
    import mrs
    mrs.main(mapper, reducer)

# vim: et sw=4 sts=4

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

def run(job, args, opts):
    source = job.file_data(args[:-1])
    intermediate = job.map_data(source, mapper)
    output = job.reduce_data(intermediate, reducer,
            outdir=args[-1], format=mrs.TextWriter)

if __name__ == '__main__':
    mrs.main(mrs.Registry(globals()), run)

# vim: et sw=4 sts=4

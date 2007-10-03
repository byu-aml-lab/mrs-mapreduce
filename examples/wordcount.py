#!/usr/bin/env python

def mapper(key, value):
    for word in value.split():
        yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

if __name__ == '__main__':
    from mrs.mapreduce import Operation, SerialJob
    import sys
    if len(sys.argv) != 2:
        print "usage: %s inputfile" % sys.argv[0]
        sys.exit(-1)
    filename = sys.argv[1]
    op = Operation(mapper, reducer, output_dir='.')
    mrsjob = SerialJob()
    mrsjob.inputs.append(filename)
    mrsjob.operations.append(op)
    mrsjob.run()

# vim: et sw=4 sts=4

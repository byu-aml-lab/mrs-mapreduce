#!/usr/bin/env python

def mapper(key, value):
    for word in value.split():
        yield (word, str(1))

def reducer(key, value_iter):
    yield str(sum(int(x) for x in value_iter))

if __name__ == '__main__':
    import mrs
    mrs.main(mapper, reducer)

"""
def wordcount_serial():
    from mrs.mapreduce import Operation, SerialJob
    import sys
    if len(sys.argv) != 2:
        print "usage: %s inputfile" % sys.argv[0]
        sys.exit(-1)
    filename = sys.argv[1]
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(output_dir='.')
    mrsjob.inputs.append(filename)
    mrsjob.operations.append(op)
    mrsjob.run()

def wordcount_posix():
    from mrs.mapreduce import Operation, POSIXJob
    import sys
    if len(sys.argv) < 2:
        print "usage: %s inputfile1 [inputfile2 ...]" % sys.argv[0]
        sys.exit(-1)
    filenames = sys.argv[1:]
    op = Operation(mapper, reducer, map_tasks=len(filenames),
            reduce_tasks=2)
    mrsjob = POSIXJob('/home/amcnabb/python/mrs/tmp')
    mrsjob.inputs = filenames
    mrsjob.operations.append(op)
    mrsjob.run()

if __name__ == '__main__':
    #wordcount_serial()
    wordcount_posix()
"""

# vim: et sw=4 sts=4

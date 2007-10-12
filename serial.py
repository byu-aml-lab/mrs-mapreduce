#!/usr/bin/env python

def run_posix(mapper, reducer, inputs, output, options):
    map_tasks = options.map_tasks
    if map_tasks == 0:
        map_tasks = len(inputs)
    if reduce_tasks == 0:
        reduce_tasks = 1

    if options.map_tasks != len(inputs):
        raise NotImplementedError("For now, the number of map tasks "
                "must equal the number of input files.")

    from mrs.mapreduce import Operation, POSIXJob
    op = Operation(mapper, reducer, map_tasks=map_tasks,
            reduce_tasks=options.reduce_tasks)
    mrsjob = POSIXJob(inputs, output, options.shared)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0


def run_serial(mapper, reducer, inputs, output, options):
    """Mrs Serial
    """
    from mrs.mapreduce import Operation, SerialJob
    op = Operation(mapper, reducer)
    mrsjob = SerialJob(inputs, output)
    mrsjob.operations = [op]
    mrsjob.run()
    return 0

# vim: et sw=4 sts=4

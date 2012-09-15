#!/usr/bin/python

from __future__ import division, print_function

import ctypes
import os
import sys
import time
import mrs

try:
    range = xrange
except NameError:
    pass

# import halton_darts from shared dll
script_dir = os.path.dirname(os.path.abspath(__file__))
so_path = os.path.join(script_dir, 'halton.so')
halton_dll = ctypes.CDLL(so_path)
halton_darts = halton_dll.halton_darts
halton_darts.restype = ctypes.c_longlong
halton_darts.argtypes = (ctypes.c_longlong, ctypes.c_longlong)

class SamplePi(mrs.MapReduce):
    """A less numerically-intensive version of the pi calculator."""

    def map(self, key, value):
        inside = halton_darts(int(value), self.opts.num_points)
        outside = self.opts.num_points - inside

        yield (str(True), str(inside))
        yield (str(False), str(outside))

    def reduce(self, key, values):
        values = list(values)
        yield str(sum(int(x) for x in values))

    def run(self, job):
        points = self.opts.num_points
        tasks = self.opts.num_tasks
        kvpairs = ((str(i), str(i * points)) for i in range(tasks))
        source = job.local_data(kvpairs, splits=tasks,
                parter=self.mod_partition)

        intermediate = job.map_data(source, self.map)
        source.close()
        output = job.reduce_data(intermediate, self.reduce)
        intermediate.close()

        job.wait(output)
        output.fetchall()
        for key, value in output.data():
            if key == 'True':
                inside = int(value)
            else:
                outside = int(value)

        pi = 4 * inside / (inside + outside)
        print(pi)
        sys.stdout.flush()

        return 0

def update_parser(parser):
    parser.add_option('-p', '--points',
                      dest='num_points', type='int',
                      help='Number of points for each map task',
                      default=1000)

    parser.add_option('-t', '--tasks',
                      dest='num_tasks', type='int',
                      help='Number of map tasks to use',
                      default=40)

    return parser

if __name__ == '__main__':
    mrs.main(SamplePi, update_parser)

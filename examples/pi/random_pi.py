#!/usr/bin/python

from __future__ import division, print_function

import mrs
import random
import sys
import time

try:
    range = xrange
except NameError:
    pass

class SamplePi(mrs.MapReduce):
    """A less numerically-intensive version of the pi calculator."""
    def map(self, key, value):
        random_ = random.random
        inside = 0

        #start = time.time()
        num_points = int(self.opts.num_points)
        for _ in range(num_points):
            x = random_() - 0.5
            y = random_() - 0.5
            if x * x + y * y <= 0.25:
                inside += 1
        #end = time.time()
        #print('Elapsed time:', end - start)

        yield (str(True), str(inside))
        yield (str(False), str(num_points - inside))

    def reduce(self, key, values):
        values = list(values)
        yield str(sum(int(x) for x in values))

    def run(self, job):
        points = int(self.opts.num_points)
        tasks = self.opts.num_tasks
        kvpairs = ((str(i), str(i * points)) for i in range(tasks))
        source = job.local_data(kvpairs)

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
                      dest='num_points',
                      help='Number of points for each map task',
                      default=1000)

    parser.add_option('-t', '--tasks',
                      dest='num_tasks', type='int',
                      help='Number of map tasks to use',
                      default=40)

    return parser

if __name__ == '__main__':
    mrs.main(SamplePi, update_parser)

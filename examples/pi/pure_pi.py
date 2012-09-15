#!/usr/bin/python

from __future__ import division, print_function

import sys
import mrs
try:
    import numpy
except ImportError:
    import numpypy as numpy

try:
    range = xrange
except NameError:
    pass

class HaltonSequence(object):

    P = [2, 3]
    K = [63, 40]

    def __init__(self, start):
        self.index = start
        self.x = numpy.zeros(len(self.K), dtype='float64')
        self.q = [numpy.zeros(n, dtype='float64') for n in self.K]
        self.d = [numpy.zeros(n, dtype='int32') for n in self.K]

        for i, n in enumerate(self.K):
            k = start
            self.x[i] = 0
            for j in range(n):
                self.q[i][j] = (1 if j == 0 else self.q[i][j - 1]) / self.P[i]
                self.d[i][j] = int(k % self.P[i])
                k = (k - self.d[i][j]) // self.P[i]
                self.x[i] += self.d[i][j] * self.q[i][j]

    def next_point(self):
        self.index += 1
        for i, n in enumerate(self.K):
            for j in range(n):
                self.d[i][j] += 1
                self.x[i] += self.q[i][j]
                if self.d[i][j] < self.P[i]:
                    break
                self.d[i][j] = 0
                self.x[i] -= 1 if j == 0 else self.q[i][j - 1]
        return tuple(self.x)

class SamplePi(mrs.MapReduce):
    def map(self, key, value):
        halton = HaltonSequence(int(value))
        inside, outside = 0, 0

        for _ in range(self.opts.num_points):
            point = halton.next_point()
            x = point[0] - .5
            y = point[1] - .5
            if x * x + y * y > .25:
                outside += 1
            else:
                inside += 1

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

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

    def __init__(self, start):
        self.index = start
        self.q0 = numpy.zeros(63, dtype='float64')
        self.d0 = numpy.zeros(63, dtype='int32')
        self.q1 = numpy.zeros(40, dtype='float64')
        self.d1 = numpy.zeros(40, dtype='int32')

        k = start
        self.x0 = 0.0
        for j in range(63):
            self.q0[j] = (1 if j == 0 else self.q0[j - 1]) / 2
            self.d0[j] = int(k % 2)
            k = (k - self.d0[j]) // 2
            self.x0 += self.d0[j] * self.q0[j]

        k = start
        self.x1 = 0.0
        for j in range(40):
            self.q1[j] = (1 if j == 0 else self.q1[j - 1]) / 3
            self.d1[j] = int(k % 3)
            k = (k - self.d1[j]) // 3
            self.x1 += self.d1[j] * self.q1[j]

    def next_point(self):
        self.index += 1
        for j in range(63):
            self.d0[j] += 1
            self.x0 += self.q0[j]
            if self.d0[j] < 2:
                break
            self.d0[j] = 0
            self.x0 -= 1 if j == 0 else self.q0[j - 1]

        for j in range(63):
            self.d1[j] += 1
            self.x1 += self.q1[j]
            if self.d1[j] < 3:
                break
            self.d1[j] = 0
            self.x1 -= 1 if j == 0 else self.q1[j - 1]
        return (self.x0, self.x1)

class SamplePi(mrs.MapReduce):
    def map(self, key, value):
        halton = HaltonSequence(value)
        inside = 0

        num_points = int(self.opts.num_points)
        for _ in range(num_points):
            point = halton.next_point()
            x = point[0] - .5
            y = point[1] - .5
            if x * x + y * y <= .25:
                inside += 1

        yield (True, inside)
        yield (False, num_points - inside)

    def reduce(self, key, values):
        yield sum(values)

    def run(self, job):
        points = int(self.opts.num_points)
        tasks = self.opts.num_tasks
        kvpairs = ((i, i * points) for i in range(tasks))
        source = job.local_data(kvpairs)

        intermediate = job.map_data(source, self.map)
        source.close()
        output = job.reduce_data(intermediate, self.reduce)
        intermediate.close()

        job.wait(output)
        output.fetchall()
        for key, value in output.data():
            if key == True:
                inside = value
            else:
                outside = value

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

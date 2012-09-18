#!/usr/bin/python

from __future__ import division, print_function

import sys
import mrs

try:
    range = xrange
except NameError:
    pass

def halton_inside(index, num_points):
    q0 = [0.0] * 63
    d0 = [0] * 63
    q1 = [0.0] * 40
    d1 = [0] * 40

    k = index
    x0 = 0.0
    for j in range(63):
        q0[j] = (1 if j == 0 else q0[j - 1]) / 2
        d0[j] = int(k % 2)
        k = (k - d0[j]) // 2
        x0 += d0[j] * q0[j]

    k = index
    x1 = 0.0
    for j in range(40):
        q1[j] = (1 if j == 0 else q1[j - 1]) / 3
        d1[j] = int(k % 3)
        k = (k - d1[j]) // 3
        x1 += d1[j] * q1[j]

    inside = 0
    for _ in range(num_points):
        index += 1

        for j in range(63):
            d0[j] += 1
            x0 += q0[j]
            if d0[j] < 2:
                break
            d0[j] = 0
            x0 -= 1 if j == 0 else q0[j - 1]

        for j in range(40):
            d1[j] += 1
            x1 += q1[j]
            if d1[j] < 3:
                break
            d1[j] = 0
            x1 -= 1 if j == 0 else q1[j - 1]

        x = x0 - .5
        y = x1 - .5
        if x * x + y * y <= .25:
            inside += 1
    return inside


class SamplePi(mrs.MapReduce):
    def map(self, key, value):
        num_points = int(self.opts.num_points)
        inside = halton_inside(value, num_points)

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

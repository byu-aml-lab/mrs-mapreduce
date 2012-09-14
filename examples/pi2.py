#!/usr/bin/python

from __future__ import division

import random
import sys
import mrs

class SamplePi(mrs.MapReduce):
    """A less numerically-intensive version of the pi calculator."""

    def map(self, key, value):
        inside = 0
        outside = 0

        for _ in range(int(value)):
            x = random.random() - 0.5
            y = random.random() - 0.5
            if x * x + y * y > 0.25:
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
        kvpairs = ((str(i * points), str(points)) for i in range(tasks))
        source = job.local_data(kvpairs, splits=tasks)

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
        print pi
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

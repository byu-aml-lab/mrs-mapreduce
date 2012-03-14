#!/usr/bin/env python3

from __future__ import division

from collections import defaultdict
import datetime
import re
import sys
import time

timestamp_pattern = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'
message_pattern = r'(?P<message>.*)'
log_re = re.compile(r'^%s: \w+: %s\s*$' % (timestamp_pattern, message_pattern))
task_pattern = r'(?P<dataset>\w+), (?P<task>\d+)'
start_re = re.compile(r'^Assigning.*: %s\s*$' % task_pattern)
stop_re = re.compile(r'^Slave \d+ report.*: %s\s*$' % task_pattern)


class Interval(object):
    def __init__(self, first):
        self.first = first
        self.last = first

    def update(self, last):
        self.last = last

    def seconds(self):
        delta = parse_timestamp(self.last) - parse_timestamp(self.first)
        return delta.total_seconds()

def parse_timestamp(timestamp):
    """Convert a timestamp string to a datetime object."""
    sec, ms = timestamp.split(',')
    microseconds = 1000 * int(ms)
    fields = time.strptime(sec, '%Y-%m-%d %H:%M:%S')[0:6] + (microseconds,)
    return datetime.datetime(*fields)

def parse_start(string):
    mo = start_re.search(string)
    if mo:
        return mo.group('dataset'), mo.group('task')
    else:
        return None, None

def parse_stop(string):
    mo = stop_re.search(string)
    if mo:
        return mo.group('dataset'), mo.group('task')
    else:
        return None, None

def load():
    datasets = []
    task_map = defaultdict(dict)

    for line in sys.stdin:
        mo = log_re.search(line)
        if mo:
            timestamp = mo.group('timestamp')
            message = mo.group('message')
            dataset, task = parse_start(message)
            if dataset:
                interval = Interval(timestamp)
                task_map[dataset][task] = interval
                if dataset not in datasets:
                    datasets.append(dataset)
            else:
                dataset, task = parse_stop(message)
                if dataset:
                    task_map[dataset][task].update(timestamp)
        else:
            print('Malformed input:')
            print('"%s"' % line)

    averages = []
    for dataset in datasets:
        intervals = task_map[dataset].values()
        average = sum(x.seconds() for x in intervals) / len(intervals)
        averages.append((dataset, average))

    return averages

def timing():
    for dataset, average in load():
        print(dataset, '  ', average)

if __name__ == '__main__':
    timing()

# vim: et sw=4 sts=4

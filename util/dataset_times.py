#!/usr/bin/env python3

import datetime
import re
import sys
import time

timestamp_pattern = r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'
message_pattern = r'(?P<message>.*)'
log_re = re.compile(r'^%s: \w+: %s\s*$' % (timestamp_pattern, message_pattern))
task_pattern = r'(?P<dataset>\w+), (?P<task>\d+)'
task_re = re.compile(r'^(Assigning|Slave \d+ report).*: %s\s*$' % task_pattern)


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

def parse_dataset(string):
    mo = task_re.search(string)
    if mo:
        return mo.group('dataset')

def load():
    intervals = []
    dataset_map = {}

    for line in sys.stdin:
        mo = log_re.search(line)
        if mo:
            timestamp = mo.group('timestamp')
            message = mo.group('message')
            dataset = parse_dataset(message)
            if dataset:
                try:
                    dataset_map[dataset].update(timestamp)
                except KeyError:
                    interval = Interval(timestamp)
                    dataset_map[dataset] = interval
                    intervals.append((dataset, interval))
        else:
            print('Malformed input:')
            print('"%s"' % line)

    return intervals

def timing():
    for dataset, interval in load():
        print(dataset, '  ', interval.seconds())

if __name__ == '__main__':
    timing()

# vim: et sw=4 sts=4

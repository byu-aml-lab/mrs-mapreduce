#!/usr/bin/env python

import argparse
import pstats

parser = argparse.ArgumentParser(description='Process Python profiler files')
parser.add_argument('-s', '--sort',
        help='sort order for pstats.Stats (e.g., "time" or "cumulative")',
        default='time')
parser.add_argument('file', nargs='+', help='profile file to import')
args = parser.parse_args()


files = iter(args.file)
stats = pstats.Stats(next(files))
for filename in files:
    stats.add(filename)

stats.strip_dirs().sort_stats(args.sort).print_stats()

# vim: et sw=4 sts=4

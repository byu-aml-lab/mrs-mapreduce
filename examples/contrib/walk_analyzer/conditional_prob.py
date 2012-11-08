#!/usr/bin/python

"""Estimate conditional probability distributions from walk data.

The point of this program is to compute random walk probabilities over a
graph.  This is a simplified problem based on `walk_analyzer.py`.  This
program finds the conditional probabilities of reaching a node given a
particular start node.  The output consists of a conditional probability
distribution for each start node.

We used a system called GraphChi to do very efficient random walks over the
graph - GraphChi can handle a billion individual walks with 10 steps over a
graph with about 2 million edges in about 20 minutes.  That is, start at 1
million separate nodes, and do 1000 walks from each node for 10 steps in
about 20 minutes, on a single (somewhat large) machine.  This process
produces a set of data files of the form (walk_id, hop_num, node_id).  From
this output, we need to create a set of probabilities as shown above.
"""

# Author: Matt Gardner (mg1@cs.cmu.edu)
# (with some help from Andrew McNabb (amcnabb@cs.byu.edu))

from __future__ import division

import itertools
import logging
import mrs
import os
import struct
from collections import defaultdict
from StringIO import StringIO
from subprocess import Popen, PIPE

NUM_PAIR_TASKS = 400
NUM_COUNT_TASKS = 300
MAX_INPUT_SIZE = 20000000
MIN_COUNT = 2

# Use the mrs logger, so we have the same log level
logger = logging.getLogger('mrs')

walk_struct = struct.Struct('>IHI')
walk_struct_size = walk_struct.size


class RandomWalkAnalyzer(mrs.MapReduce):
    def run(self, job):
        outdir = self.output_dir()
        if not outdir:
            return 1

        # This is the main part of the program, that gets run on the master.

        # This is the initial data (in (key, value) format) that is sent to
        # the map.  In our case, we just need to give an index to the map task,
        # and each mapper will look up the document it needs from that index.
        kv_pairs = []
        for filename in self.args[:-1]:
            size = os.stat(filename).st_size
            assert size % walk_struct_size == 0
            total_records = size // walk_struct_size
            chunks = (size - 1) // MAX_INPUT_SIZE + 1

            offset = 0
            for i in xrange(chunks):
                chunk_records = total_records // chunks
                # Spread out the remainder among the first few chunks.
                if i < total_records % chunks:
                    chunk_records += 1
                key = filename
                value = (offset, chunk_records)
                kv_pairs.append((key, value))
                offset += chunk_records

        source = job.local_data(kv_pairs)

        # We pass the initial data into the map tasks
        walk_ids = job.map_data(source, self.walk_file_map,
                parter=self.mod_partition, splits=NUM_PAIR_TASKS)
        source.close()

        # If the output of a reduce is going straight into a map, we can do a
        # reducemap, which is pretty nice.
        node_pairs = job.reducemap_data(walk_ids, self.walk_id_reduce,
                self.node_pair_map, splits=NUM_COUNT_TASKS)
        walk_ids.close()

        # We just output here, which leads to pretty ugly storing of the
        # output in an arbitrary directory structure.  The alternative is to
        # grab it after it's done and do whatever outputting you want in this
        # run() method, but then you have to hope that all of the data fits in
        # memory.  Because we think this output will be rather large, we do
        # our outputting directly from the reduce.
        output = job.reduce_data(node_pairs, self.normalize_reduce,
                outdir=outdir, format=mrs.fileformats.TextWriter)
        node_pairs.close()

        ready = []
        while not ready:
            ready = job.wait(output, timeout=10.0)
            logger.info('Walk ids: ' + str(job.progress(walk_ids)))
            logger.info('Node pairs: ' + str(job.progress(node_pairs)))
            logger.info('Output: ' + str(job.progress(output)))

        # If you don't return 0, mrs thinks your job failed
        return 0

    int32_serializer = mrs.make_primitive_serializer('I')
    int32_pair_serializer = mrs.make_struct_serializer('=II')

    @mrs.output_serializers(key=int32_serializer, value=int32_pair_serializer)
    def walk_file_map(self, key, value):
        """Process the input files, emitting one entry for each step."""
        filename = key
        offset, count = value
        logger.info('Got walk file %s (offset %s, count %s)' %
                (filename, offset, count))
        walk_file = open(filename, 'rb')
        walk_file.seek(offset * walk_struct_size)

        for i in xrange(count):
            walk_buf = walk_file.read(walk_struct_size)
            walk_id, hop, node = walk_struct.unpack(walk_buf)
            yield (walk_id, (hop, node))

    def walk_id_reduce(self, key, values):
        """Consolidate each walk into a single list of nodes."""
        value_list = list(itertools.islice(values, 100))
        # GraphChi shouldn't ever let this happen, but sometimes there is a
        # single walk_id with a pathologically long list of hops that really
        # breaks things in map_walk_ids.  So we catch that case here.
        if len(value_list) < 100:
            value_list.sort()
            nodes = [node for hop, node in value_list]
            yield nodes

    @mrs.output_serializers(key=int32_serializer, value=int32_serializer)
    def node_pair_map(self, key, value):
        """Emit an entry for each pair of nodes in the walks."""
        for i, start_node in enumerate(value):
            for end_node in value[i+1:]:
                yield (start_node, end_node)

    def normalize_reduce(self, key, values):
        """Make a conditional probability distribution given the node `key`."""
        counts = defaultdict(int)
        for v in values:
            counts[v] += 1

        distribution = {}
        total = 0
        for node, count in counts.iteritems():
            if count >= MIN_COUNT:
                distribution[node] = count
                total += count

        for node in distribution:
            distribution[node] /= total

        if distribution:
            yield distribution


if __name__ == '__main__':
    mrs.main(RandomWalkAnalyzer)

# vim: et sw=4 sts=4

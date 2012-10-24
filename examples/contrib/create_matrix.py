#!/usr/bin/python

# Author: Matt Gardner (mg1@cs.cmu.edu)
# (with some help from Andrew McNabb (amcnabb@cs.byu.edu))
#
# The point of this program is to compute random walk probabilities over a
# graph.  The quantity I am interested in is the probability of reaching a
# particular target node after starting in a given location and following a
# particular path: p(target_node | start_node, path).  We use these
# probabilities as features in a machine learning system to try to predict
# relationships between nodes in the graph.  For instance, say you want to
# predict what country Pittsburgh is in.  Pittsburgh is a node in the graph,
# and "city in country" is an edge type.  You want to pick node t as follows:
#
# argmax_t \sum_path p(t | "Pittsburgh", path) * weight(path)
#
# where you have some number of training examples so you can pick paths that
# always end up at countries and learn weights for each path.
#
# Anyway, that's the end goal.  Here, we used a system called GraphChi to do
# very efficient random walks over the graph - GraphChi can handle a billion
# individual walks with 10 steps over a graph with about 2 million edges in
# about 20 minutes.  That is, start at 1 million separate nodes, and do 1000
# walks from each node for 10 steps in about 20 minutes, on a single (somewhat
# large) machine.  This process produces a set of data files of the form
# (walk_id, hop_num, node_id).  From this output, we need to create a set of
# probabilities as shown above.
#
# To do that, we use the sequence of maps and reduces shown below (though
# currently this doesn't actually normalize the output to probabilities, nor
# does it format it nicely in a matrix).  In summary, these are the steps:
#
# input files --map--> walk_id, (hop_num, node_id)
# walk_id, (hop_num, node_id) --reduce--> walk_id, list(hop_num, node_id)
# walk_id, list(hop_num, node_id) --map--> (source_node, end_node), path
# (source_node, end_node), path --reduce-->
#                                    (source_node, end_node), counter(path)
#
# To normalize the probabilities, we would add another map and reduce:
#
# (source_node, end_node), counter(path) --map-->
#                                   (source_node, path), (target_node, count)
# (source_node, path), (target_node, count) --reduce-->
#                             (source_node, target_node), (path, probability)
#
# There are some issues I care about that aren't implemented here, such as
# actually putting in the edge types that exist between pairs of nodes, and
# doing various kinds of filtering on paths to only keep paths I care about.
# But this is a starting place that does the basic work of producing the
# probability table I'm interested in.

import logging
import mrs
import os
import struct
from collections import defaultdict
from StringIO import StringIO
from subprocess import Popen, PIPE

NUM_PAIR_TASKS = 450
NUM_COUNT_TASKS = 350
MAX_INPUT_SIZE = 10000000
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
            total_records = size / walk_struct_size
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
        walk_ids = job.map_data(source, self.map_walk_files,
                parter=self.mod_partition, splits=NUM_PAIR_TASKS)
        source.close()

        # If the output of a reduce is going straight into a map, we can do a
        # reducemap, which is pretty nice.
        node_pairs = job.reducemap_data(walk_ids, self.collapse_reduce,
                self.map_walk_ids, splits=NUM_COUNT_TASKS)
        walk_ids.close()

        # We just output here, which leads to pretty ugly storing of the
        # output in an arbitrary directory structure.  The alternative is to
        # grab it after it's done and do whatever outputting you want in this
        # run() method, but then you have to hope that all of the data fits in
        # memory.  Because we think this output will be rather large, we do
        # our outputting directly from the reduce.
        output = job.reduce_data(node_pairs, self.count_reduce, outdir=outdir,
                format=mrs.fileformats.TextWriter)
        node_pairs.close()

        ready = []
        while not ready:
            ready = job.wait(output, timeout=10.0)
            logger.info('Walk ids: ' + str(job.progress(walk_ids)))
            logger.info('Node pairs: ' + str(job.progress(node_pairs)))
            logger.info('Output: ' + str(job.progress(output)))

        # If you don't return 0, mrs thinks your job failed
        return 0

    def map_walk_files(self, key, value):
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

    def map_walk_ids(self, key, value):
        for i, (beg_hop, beg_node) in enumerate(value):
            prev_node = beg_node
            path = str(beg_node)
            for (end_hop, end_node) in value[i+1:]:
                # TODO: look up edge type between prev_node and end_node, and
                # otherwise make path nice
                path += '-' + str(end_node)
                yield ('%d-%d' % (beg_node, end_node), path)

    def collapse_reduce(self, key, values):
        v = list(values)
        # GraphChi shouldn't ever let this happen, but sometimes there is a
        # single walk_id with a pathologically long list of hops that really
        # breaks things in map_walk_ids.  So we catch that case here.
        if len(v) < 100:
            v.sort()
            yield v

    def count_reduce(self, key, values):
        counts = defaultdict(int)
        for v in values:
            counts[v] += 1
        outdict = {}
        for path, count in counts.iteritems():
            if count >= MIN_COUNT:
                outdict[path] = count
        if outdict:
            yield str(outdict)


if __name__ == '__main__':
    mrs.main(RandomWalkAnalyzer)

# vim: et sw=4 sts=4

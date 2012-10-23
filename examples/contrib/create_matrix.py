#!/usr/bin/python

import logging
import mrs
import os
import struct
from collections import defaultdict
from StringIO import StringIO
from subprocess import Popen, PIPE

NUM_TASKS = 500
MAX_INPUT_SIZE = 20000000

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

        # This is how you create the initial data to pass to the mappers (we
        # use a mod partition because we have numerical data that is guaranteed
        # to be sequential - see the note in mapreduce.py about that).
        source = job.local_data(kv_pairs)

        # We pass the initial data into the map tasks
        walk_ids = job.map_data(source, self.map_walk_files,
                parter=self.mod_partition, splits=NUM_TASKS)
        source.close()

        node_pairs = job.reducemap_data(walk_ids, self.collapse_reduce,
                self.map_walk_ids, splits=NUM_TASKS)
        walk_ids.close()

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
                # TODO: look up edge type between prev_node and end_node
                edge_type = 'edge'
                path += '-' + edge_type + '-' + str(end_node)
                yield ('%d-%d' % (beg_node, end_node), path)

    def collapse_reduce(self, key, values):
        v = list(values)
        if len(v) < 100:
            v.sort()
            yield v

    def count_reduce(self, key, values):
        counts = defaultdict(int)
        for v in values:
            counts[v] += 1
        yield counts


if __name__ == '__main__':
    mrs.main(RandomWalkAnalyzer)

# vim: et sw=4 sts=4

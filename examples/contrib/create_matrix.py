#!/usr/bin/python

import logging
import mrs
import os
import struct
from collections import defaultdict
from StringIO import StringIO
from subprocess import Popen, PIPE

# Use the mrs logger, so we have the same log level
logger = logging.getLogger('mrs')

class RandomWalkAnalyzer(mrs.MapReduce):
    def run(self, job):
        outdir = self.output_dir()
        if not outdir:
            return 1

        # This is the main part of the program, that gets run on the master.

        #num_tasks = 1000
        num_tasks = 500

        # This is the initial data (in (key, value) format) that is sent to
        # the map.  In our case, we just need to give an index to the map task,
        # and each mapper will look up the document it needs from that index.
        documents = enumerate(self.args[:-1])

        # This is how you create the initial data to pass to the mappers (we
        # use a mod partition because we have numerical data that is guaranteed
        # to be sequential - see the note in mapreduce.py about that).
        source = job.local_data(documents)

        # We pass the initial data into the map tasks
        walk_ids = job.map_data(source, self.map_walk_files,
                parter=self.mod_partition, splits=num_tasks)
        source.close()

        node_pairs = job.reducemap_data(walk_ids, self.collapse_reduce,
                self.map_walk_ids, splits=num_tasks)
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
        logger.info('Got walk file ' + value)
        walk_file = open(value, 'rb')
        walk_file.seek(0, 2)
        file_size = walk_file.tell()
        logger.debug('File size: ' + str(file_size))
        out_rate = file_size / 100
        walk_file.seek(0, 0)
        while file_size - walk_file.tell() > 0:
            if walk_file.tell() % out_rate == 0:
                logger.debug('At position: ' + str(walk_file.tell()))
            walk_struct = walk_file.read(10)
            walk_id, hop, node = struct.unpack('>IHI', walk_struct)
            yield (walk_id, (hop, node))

    def map_walk_ids(self, key, value):
        #logger.debug('Processing walk id ' + str(key))
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

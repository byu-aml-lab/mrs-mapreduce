#!/usr/bin/python

# Author: Matt Gardner (mg1@cs.cmu.edu)
# (with some help from Andrew McNabb (amcnabb@cs.byu.edu))
#
# The point of this program is to compute random walk probabilities over a
# graph.  The quantity I am interested in is the probability of reaching a
# particular target node after starting in a given location and following a
# particular path: p(target_node | start_node, path).  We use these
# probabilities as features in a machine learning system to try to predict
# relationships between nodes in the graph.  Also, note that for this entire
# file, we don't use "path" in the graph theory sense (a sequence of nodes), we
# just mean something more like "path type", and generally just a sequence of
# edge types.  To use formal graph theory notation, the probability is actually
# p(reaching target node | starting at start_node and following a path that
# matches some set of features).  But, to be concise in our notation, we just
# use "path" to mean "any (graph theory) path with a particular set of
# features."
#
# For instance, say you want to predict what country Pittsburgh is in.
# Pittsburgh is a node in the graph, and "cityInCountry" is an edge type.
# You want to pick node t as follows:
#
# argmax_t \sum_path p(t | "Pittsburgh", path) * weight(path)
#
# where you have some number of training examples so you can pick paths that
# always end up at countries and learn weights for each path.  One example path
# might be "cityOnRiver -> cityOnRiver^-1 -> cityInCountry", taking you from
# Pittsburgh to the river it's on, to another city in the same river, to the
# country that the other city is in.  Intuitively, if this path has a high
# weight, that means the machine learning system thinks that cities that are on
# the same river are likely in the same country.
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
# To normalize the probabilities, we add another map and reduce:
#
# (source_node, end_node), counter(path) --map-->
#                                   (source_node, path), (target_node, count)
# (source_node, path), (target_node, count) --reduce-->
#                             (source_node, target_node), (path, probability)

from __future__ import division

import itertools
import logging
import mrs
import os
import struct
from collections import defaultdict
from StringIO import StringIO
from subprocess import Popen, PIPE

# Use the mrs logger, so we have the same log level
logger = logging.getLogger('mrs')

walk_struct = struct.Struct('>IHI')
walk_struct_size = walk_struct.size

int32_struct = struct.Struct('=I')

def int32_bytes_dumps(pair):
    """Pack a pair consisting of an int and a bytes."""
    i, b = pair
    return int32_struct.pack(i) + b

def int32_bytes_loads(x):
    """Unpack a pair consisting of an int and a bytes."""
    i = int32_struct.unpack(x[:int32_struct.size])
    b = x[int32_struct.size:]
    return i, b


class RandomWalkAnalyzer(mrs.MapReduce):

    def __init__(self, opts, args):
        super(RandomWalkAnalyzer, self).__init__(opts, args)
        self.rel_names = {}
        self.node_names = {}
        for line in open(opts.rel_names_file):
            source, target, name = line.strip().split("\t")
            source = int(source)
            target = int(target)
            self.rel_names[(source, target)] = name
            self.rel_names[(target, source)] = name + "_inv"
        for line in open(opts.node_names_file):
            node, name = line.strip().split("\t")
            node = int(node)
            # This maybe could be an array, if it's faster, but I'm not sure
            # that we're guaranteed to have every index filled, so it would be
            # at least slightly tricky to create the array appropriately.
            self.node_names[node] = name

    def run(self, job):
        outdir = self.output_dir()
        if not outdir:
            return 1

        # This is the main part of the program, that gets run on the master.

        # This is the initial data (in (key, value) format) that is sent to
        # the map.  In our case, we just need to give an index to the map task,
        # and each mapper will look up the document it needs from that index.
        kv_pairs = []
        max_input_size = self.opts.input_chunk_size * 1024 ** 2
        for filename in self.args[:-1]:
            size = os.stat(filename).st_size
            assert size % walk_struct_size == 0
            total_records = size // walk_struct_size
            chunks = (size - 1) // max_input_size + 1

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
                parter=self.mod_partition, splits=self.opts.num_pair_tasks)
        source.close()

        # If the output of a reduce is going straight into a map, we can do a
        # reducemap, which is pretty nice.
        node_pairs = job.reducemap_data(walk_ids, self.walk_id_reduce,
                self.node_pair_map, splits=self.opts.num_count_tasks)
        walk_ids.close()

        path_counts = job.reducemap_data(node_pairs, self.path_count_reduce,
                self.source_path_map, splits=self.opts.num_output_tasks)
        node_pairs.close()

        # We just output here, which leads to pretty ugly storing of the
        # output in an arbitrary directory structure.  The alternative is to
        # grab it after it's done and do whatever outputting you want in this
        # run() method, but then you have to hope that all of the data fits in
        # memory.  Because we think this output will be rather large, we do
        # our outputting directly from the reduce.
        output_matrix = job.reducemap_data(path_counts, self.normalize_reduce,
                self.matrix_map, splits=1,
                outdir=outdir, format=mrs.fileformats.TextWriter)
        path_counts.close()

        ready = []
        while not ready:
            ready = job.wait(output_matrix, timeout=5.0)
            logger.info('Walk ids: ' + str(job.progress(walk_ids)))
            logger.info('Node pairs: ' + str(job.progress(node_pairs)))
            logger.info('Path counts: ' + str(job.progress(path_counts)))
            logger.info('Output matrix: ' + str(job.progress(output_matrix)))

        # If you don't return 0, mrs thinks your job failed
        return 0

    int32_serializer = mrs.make_primitive_serializer('=I')
    int32_pair_serializer = mrs.make_struct_serializer('=II')

    @mrs.output_serializers(key=int32_serializer, value=int32_pair_serializer)
    def walk_file_map(self, key, value):
        """Input is the walk file, output is walk_id, (hop, node)"""
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
        """Input is walk_id, (hop, node), from walk_file_map.  Output is
        walk_id, list(node)."""
        value_list = list(itertools.islice(values, 100))
        # GraphChi shouldn't ever let this happen, but sometimes there is a
        # single walk_id with a pathologically long list of hops that really
        # breaks things in map_walk_ids.  So we catch that case here.
        if len(value_list) < 100:
            value_list.sort()
            nodes = [node for hop, node in value_list]
            yield nodes

    # Note: this program isn't Python 3 compatible anyway, so raw_serializer
    # should be a bit faster than str_serializer.
    #@mrs.output_serializers(key=int32_pair_serializer, value='str_serializer')
    @mrs.output_serializers(key=int32_pair_serializer, value='raw_serializer')
    def node_pair_map(self, key, value):
        """Input is walk_id, list(node), from walk_id_reduce.  We then ignore
        walk_id and output (start_node, end_node), path, by going though the
        list of nodes.  We have to do some lookups to another data file to get
        edge types between given node pairs."""
        for i, start_node in enumerate(value):
            path = Path()
            path.add_node(start_node)
            prev_node = start_node
            for node in value[i+1:]:
                # TODO: fix the paths output here
                edge = self.rel_names.get((prev_node, node), 'UNKNOWN_EDGE')
                path.add_edge(edge)
                path_str = path.get_path_string()
                if path_str:
                    yield ((start_node, node), path_str)
                path.add_node(node)
                prev_node = node

    def path_count_reduce(self, key, values):
        """Input is (start_node, end_node), path, from node_pair_map.  We
        aggregate all of the paths between the two nodes into a counter and
        output (start_node, end_node), counter(path)."""
        counts = defaultdict(int)
        for v in values:
            counts[v] += 1
        outdict = {}
        for path, count in counts.iteritems():
            if count >= self.opts.min_path_count:
                outdict[path] = count
        if outdict:
            yield outdict

    int32_bytes_serializer = mrs.Serializer(int32_bytes_dumps,
            int32_bytes_loads)

    @mrs.output_serializers(key=int32_bytes_serializer,
            value=int32_pair_serializer)
    def source_path_map(self, key, value):
        """Key here is (source_node, target_node), and value is the counter
        from path_count_reduce.  The output is (source_node, path),
        (target_node, count), so we can normalize over (source_node, path)."""
        source_node, target_node = key
        for path, count in value.iteritems():
            yield ((source_node, path), (target_node, count))

    def normalize_reduce(self, key, values):
        """Input key is (source_node, path), and values is a list of
        (target_node, count).  We normalize the counts to be probabilities, and
        output (target_node, (count, total_count, num_targets)) (the extra
        information is for consumers of these probabilities, for judging their
        reliability)."""
        total_count = 0
        # I need to iterate over values twice, so we need to make a persistent
        # copy of it.
        values = list(values)
        for target, count in values:
            total_count += count
        if total_count < self.opts.min_total_count:
            return
        output = dict()
        for target, count in values:
            # In addition to saving the actually probability, we save a couple
            # of other numbers to aid the consumer of this probability in
            # judging how reliably it was estimated.  If
            # self.opts.min_total_count is high enough, this may be
            # unnecessary.
            yield (target, (count / total_count, total_count, len(values)))

    def matrix_map(self, key, value):
        """Input is (source_node, path), (target, (count + stuff)).  We want to
        convert this to (source_node, target_node), (path, count + stuff), so
        that it's in a nice form for easy lookups by node pair.  That's all
        this does."""
        source_node, path = key
        source_node = self.node_names.get(source_node, 'UNKNOWN_NODE')
        target_node, stats = value
        target_node = self.node_names.get(target_node, 'UNKNOWN_NODE')
        key = source_node + " " + target_node
        value = path + " %.5f %d %d" % stats
        yield (key, value)

    @classmethod
    def update_parser(cls, parser):
        parser.add_option('', '--rel-file',
                dest='rel_names_file',
                help='Path to the relation names file',
                )
        parser.add_option('', '--node-file',
                dest='node_names_file',
                help='Path to the node names file',
                )
        parser.add_option('', '--min-path-count',
                dest='min_path_count', type=int,
                help='Minimum number of times a particular (source, path, '
                    'target) triple must be traversed to be kept',
                default=100,
                )
        parser.add_option('', '--min-total-count',
                dest='min_total_count', type=int,
                help='Minimum number of times a particular (source, path) '
                    'pair must be traversed to be kept',
                default=300,
                )
        parser.add_option('', '--num-pair-tasks',
                dest='num_pair_tasks', type=int,
                help='Number of tasks (ideally prime) for '
                    'walk_id_reduce_node_pair_map',
                default=1499,
                )
        parser.add_option('', '--num-count-tasks',
                dest='num_count_tasks', type=int,
                help='Number of tasks (ideally prime) for '
                    'path_count_reduce_source_path_map',
                default=1499,
                )
        parser.add_option('', '--num-output-tasks',
                dest='num_output_tasks', type=int,
                help='Number of tasks (ideally prime) for '
                    'normalize_reduce_matrix_map',
                default=499,
                )
        parser.add_option('', '--input-chunk-size',
                dest='input_chunk_size', type=int,
                help='The amount of data (in MB) for each input map task',
                default=64,
                )
        return parser


class Path(object):
    def __init__(self):
        # We assume here that nodes are of type int and edges are of type str;
        # that's how we differentiate them in other parts of the code.
        self.items = []

    def add_node(self, node):
        self.items.append(node)

    def add_edge(self, edge):
        self.items.append(edge)

    def get_path_string(self, remove_cycles=True, lexicalize=False,
            max_length=5):
        to_output = []
        i = len(self.items) - 1
        while i >= 0:
            item = self.items[i]
            if lexicalize or not isinstance(item, int):
                to_output.append(item)
            if isinstance(item, int):
                j = i - 1
                while j >= 0:
                    if self.items[j] == self.items[i]:
                        i = j
                    j -= 1
            i -= 1
        if len(to_output) > max_length or not to_output:
            return None
        to_output.reverse()
        return '-'.join(to_output)


if __name__ == '__main__':
    mrs.main(RandomWalkAnalyzer)

# vim: et sw=4 sts=4

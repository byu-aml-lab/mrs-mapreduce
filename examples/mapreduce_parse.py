#!/usr/bin/python
# Mrs
# Copyright 2008-2012 Brigham Young University
#
# This file is part of Mrs.
#
# Mrs is free software: you can redistribute it and/or modify it under the
# terms of the GNU General Public License as published by the Free Software
# Foundation, either version 3 of the License, or (at your option) any later
# version.
#
# Mrs is distributed in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# Mrs.  If not, see <http://www.gnu.org/licenses/>.
#
# Inquiries regarding any further use of Mrs, please contact the Copyright
# Licensing Office, Brigham Young University, 3760 HBLL, Provo, UT 84602,
# (801) 422-9339 or 422-3821, e-mail copyright@byu.edu.


################################################################################
#
# This file is not meant to be a completely working example; there are other
# dependencies that we are not including, because this is not runnable without
# a lot of other code and set up.  However, this is an example of a slightly
# more complicated MapReduce program than WordCount.
#
# This example demonstrates how to take code that was written to examine many
# individual files in serial and make it into a MapReduce program.  If the code
# that does the examining is easily importable, you simply need a little
# wrapper around it that puts it into MapReduce.
#
################################################################################


# The original application was interacting with a Django app, so this makes
# that work.
import os, sys
sys.path.append('/aml/home/mjg82/clone/topicvisualizer/trunk/')
os.environ['DJANGO_SETTINGS_MODULE'] = 'topic_modeling.settings'
from topic_modeling import settings
# The mappers and reducers ran on separate machines, and the database they
# needed to access was local to the machine named genius.  The slave machines
# could access the database on /genius, where it was mounted.
settings.DATABASE_NAME = '/genius/mjg82/yamba'

import cjson, mrs, pickle
from collections import defaultdict
from parse_dependencies import parse_file, TopicInfo
from StringIO import StringIO
from subprocess import Popen, PIPE

from topic_modeling.visualize.models import Dataset

class DependencyParse(mrs.MapReduce):
    
    def __init__(self, opts, args):
        super(DependencyParse, self).__init__(opts, args)

        # Save some state so that the mappers can access the database, as they
        # need to.  This gets run on both the master and the slave, so
        # attributes saved here are available later.  That is not true of
        # attributes saved in the map and reduce methods.
        self.dataset = Dataset.objects.get(name=opts.dataset)
        self.analysis = self.dataset.analysis_set.get(name=opts.analysis)
        self.outdir = opts.outdir
        # We are using Popen to run a parser on the files, so we save the
        # needed command here.
        self.cwd = '/aml/home/mjg82/clone/stanfordparser/'
        args = ['java', '-mx3000m', '-cp']
        args.append('"./stanford-parser.jar:"')
        args.append('edu.stanford.nlp.parser.lexparser.LexicalizedParser')
        args.append('-outputFormat')
        args.append('"typedDependencies"')
        args.append('./englishPCFG.ser.gz')
        self.command = ' '.join(args)

    def run(self, job):
        # This is the main part of the program, that gets run on the master.

        # These determine how many map and reduce tasks we will have, and how
        # many output files there will be.
        num_docs = self.dataset.document_set.count()
        num_reduce_tasks = 5
        num_topics = self.analysis.topic_set.count()
        # This is the initial data (in (key, value) format) that is sent to
        # the map.  In our case, we just need to give an index to the map task,
        # and each mapper will look up the document it needs from that index.
        documents = [(str(i), str(i)) for i in range(num_docs)]
        # This is how you create the initial data to pass to the mappers (we
        # use a mod partition because we have numerical data that is guaranteed
        # to be sequential - see the note in mapreduce.py about that).
        data = job.local_data(documents, parter=self.mod_partition,
                splits=num_docs)

        # We pass the initial data into the map tasks
        intermediate = job.map_data(data, self.map, parter=self.mod_partition,
                splits=num_reduce_tasks)

        # We take the output of the map task and give it to the reducers.  We
        # don't have to wait here, or anything, because the MapReduce framework
        # takes care of that for us.  Just pass the data into the reduce task.
        output = job.reduce_data(intermediate, self.reduce, outdir=self.outdir,
                format=mrs.io.textformat.TextWriter, splits=num_topics,
                parter=self.mod_partition)

        # After one map and one reduce, we are done.  If we had needed more
        # complicated processing, like more maps and/or reduces, we would have
        # called job.map_data or job.reduce_data with more intermediates.  But
        # we don't here, so we just end the job.
        job.end()

        # This is where the magic happens; when the job is finished, ready will
        # no longer be an empty list, and the program will terminate.
        ready = []
        while not ready:
            ready = job.wait(output, timeout=2.0)
            print job.status()

        # We could have done something with ready at this point, instead of
        # specifying outdir and format above.  But, we just output the reduce
        # information to file, for processing later in another script.

    def map(self, key, value):
        # This is really simple.  Just get the document we're assigned from the
        # the database.
        document = self.dataset.document_set.all()[int(value)]
        filename = self.dataset.data_root + '/' + document.filename
        markup_file = self.dataset.data_root + '/' + document.markup_file
        topic_info = defaultdict(TopicInfo)
        # Then parse it, populating topic_info in the process
        parse_file(filename, markup_file, self.command, topic_info, self.cwd)
        # Now output topic_info for the reduce tasks.  Our parsing method finds
        # information in each document about a specified number of topics, then
        # outputs that information.  The reduce combines information about each
        # topic found in all of the documents.
        for topic in topic_info:
            s = StringIO()
            pickle.dump(topic_info[topic], s)
            yield (str(topic), s.getvalue())

    def reduce(self, key, values):
        # All we do is aggregate all of the information we've seen
        topic_info = TopicInfo()
        for value in values:
            s = StringIO(value)
            info = pickle.load(s)
            topic_info.aggregate(info)
        s = StringIO()
        pickle.dump(topic_info, s)
        # Then output it in as a pickle, for easy analysis later.
        yield s.getvalue()


def update_parser(parser):
    parser.add_option('-d', '--dataset',
            dest='dataset',
            help='Database name of the dataset to use',
            )
    parser.add_option('-a', '--analysis',
            dest='analysis',
            help='Database name of the analysis to use',
            )
    parser.add_option('-o', '--outdir',
            dest='outdir',
            help='Directory to store the output',
            )
    return parser


if __name__ == '__main__':
    mrs.main(DependencyParse, update_parser)

# vim: et sw=4 sts=4

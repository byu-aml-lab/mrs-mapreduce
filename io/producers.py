# Mrs
# Copyright 2008-2011 Brigham Young University
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

# If BLOCKSIZE is too big, then latency will be high; if it's too low, then
# throughput will be low.
BLOCKSIZE = 1000

TEST_HOST = 'cs.byu.edu'


class SerialProducer(object):
    """A Producer which reads data from any URL.

    The producer will do all work when the run method is called, and it does
    not interact with the reactor in any way.  This producer should only be
    used in sequential code or in a non-reactor thread.
    """

    def __init__(self, url, consumer):
        self.url = url
        consumer.registerProducer(self, streaming=True)
        self.consumer = consumer

    def run(self):
        """Loads data and sends it to the consumer."""
        from load import open_url
        f = open_url(self.url)
        data = f.read()
        f.close()
        self.consumer.write(data)
        self.consumer.unregisterProducer()


# vim: et sw=4 sts=4

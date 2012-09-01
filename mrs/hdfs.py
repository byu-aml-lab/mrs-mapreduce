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

"""Hadoop Distributed File System WebHDFS Client"""

from __future__ import division

import httplib
import json
import urllib


class WebHDFSProxy(object):
    """Connect to a HDFS server over webhdfs.

    Assumes that authentication is disabled on the server (it believes
    whatever username you give).
    """
    def __init__(self, addr, username, port=50070):
        self._namenode_conn = httplib.HTTPConnection(addr, port)
        self._homedir = None
        self._username = username

    def get_home_directory(self):
        """Returns the path to the home directory of the configured user."""
        if self._homedir is None:
            response = self._namenode_get('/', 'GETHOMEDIRECTORY')
            path_json = response.read()
            path_object = json.loads(path_json)
            self._homedir = path_object['Path']
        return self._homedir

    def list_status(self, path):
        """List a directory.

        Returns a list of dictionaries, one for each file.  Each dictionary
        includes the keys "accessTime", "blockSize", "group", "length",
        "modificationTime", "owner", "pathSuffix", "permission",
        "replication", and "type".  If the path is not absolute (starting with
        '/'), then it's relative to the user's home directory.
        """
        response = self._namenode_get(path, 'LISTSTATUS')
        filestatuses_json = response.read()
        filestatuses_object = json.loads(filestatuses_json)
        return filestatuses_object['FileStatuses']['FileStatus']

    def _namenode_get(self, path, op, args=None):
        """Send a GET request to the namenode.

        Returns the HTTPResponse object, which is filelike. Note that this
        response object must be fully read before beginning to read any
        subsequent response.
        """
        request_uri = self._request_uri(path, op, args)
        self._namenode_conn.request('GET', request_uri)
        response = self._namenode_conn.getresponse()
        return response

    def _request_uri(self, path, op, args=None):
        """Builds a webhdfs request URI from a path, operation, and args.

        The `args` argument is a dictionary used to construct the query. All
        parts of the resulting request URI are properly quoted.
        """
        assert path.startswith('/')
        quoted_path = urllib.quote('/webhdfs/v1' + path)

        query = {'op': op,
                'user.name': self._username}
        if args:
            query.update(args)
        quoted_query = urllib.urlencode(query)

        request_uri = '%s?%s' % (quoted_path, quoted_query)
        return request_uri


# vim: et sw=4 sts=4

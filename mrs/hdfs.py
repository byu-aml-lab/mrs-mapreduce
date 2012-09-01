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

"""Hadoop Distributed File System WebHDFS Client

The protocol specification is defined at:
    http://hadoop.apache.org/common/docs/r1.0.0/webhdfs.html
"""

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

    ##########################################################################
    # Get Methods

    def open(self, path, **args):
        """Read a file.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('GET', path, 'OPEN', args)
        response.read()
        check_code(response.status, content, httplib.TEMPORARY_REDIRECT)
        datanode_url = response.getheader('Location')

        response = self._datanode_get(datanode_url)
        content = response.read()
        check_code(response.status, content)
        return content

    def get_home_directory(self):
        """Returns the path to the home directory of the configured user."""
        if self._homedir is None:
            response = self._namenode_request('GET', '/', 'GETHOMEDIRECTORY')
            content = response.read()
            check_code(response.status, content)
            path_json = json.loads(content)
            self._homedir = path_json['Path']
        return self._homedir

    def get_file_status(self, path):
        """List a directory.

        Returns a dictionaries which contains the keys "accessTime",
        "blockSize", "group", "length", "modificationTime", "owner",
        "pathSuffix", "permission", "replication", and "type".  If the path is
        not absolute (starting with '/'), then it's relative to the user's
        home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('GET', path, 'GETFILESTATUS')
        content = response.read()
        check_code(response.status, content)
        filestatuses_json = json.loads(content)
        return filestatuses_json['FileStatus']

    def list_status(self, path):
        """List a directory.

        Returns a list of dictionaries, one for each file.  Each dictionary
        includes the keys "accessTime", "blockSize", "group", "length",
        "modificationTime", "owner", "pathSuffix", "permission",
        "replication", and "type".  If the path is not absolute (starting with
        '/'), then it's relative to the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('GET', path, 'LISTSTATUS')
        content = response.read()
        check_code(response.status, content)
        filestatuses_json = json.loads(content)
        return filestatuses_json['FileStatuses']['FileStatus']

    def get_contents_summary(self, path):
        """Get content summary of a directory.

        Returns a dictionary containing the keys "directoryCount",
        "fileCount", "length", "quota", "spaceConsumed", and "spaceQuota".  If
        the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('GET', path, 'LISTSTATUS')
        content = response.read()
        check_code(response.status, content)
        filestatuses_json = json.loads(content)
        return filestatuses_json['FileStatuses']['FileStatus']

    ##########################################################################
    # Put/Delete Methods

    # Unlike the other commands, CREATE and APPEND require a two-step process
    # to ensure that data is not unnecessarily sent to the namenode.

    def create(self, path, data, **args):
        """Create and write to a file.

        The `data` parameter can be either a string or a file (but not
        necessarily a filelike in general--see the httplib docs).  If the path
        is not absolute (starting with '/'), then it's relative to the user's
        home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('PUT', path, 'CREATE', args)
        response.read()
        check_code(response.status, content, httplib.TEMPORARY_REDIRECT)
        datanode_url = response.getheader('Location')

        response = self._datanode_request('PUT', datanode_url, data)
        content = response.read()
        check_code(response.status, content, httplib.CREATED)

    def append(self, path, data, **args):
        """Append to a file.

        Note that the HDFS server may or may not support appending.  The
        `data` parameter can be either a string or a file (but not necessarily
        a filelike in general--see the httplib docs).  If the path is not
        absolute (starting with '/'), then it's relative to the user's home
        directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('PUT', path, 'APPEND', args)
        response.read()
        check_code(response.status, content, httplib.TEMPORARY_REDIRECT)
        datanode_url = response.getheader('Location')

        response = self._datanode_request('PUT', datanode_url, data)
        content = response.read()
        check_code(response.status, content, httplib.OK)

    def mkdirs(self, path, **args):
        """Make a directory.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('PUT', path, 'MKDIRS', args)
        content = response.read()
        check_code(response.status, content)
        boolean_json = json.loads(content)
        return boolean_json['boolean']

    def rename(self, path1, path2):
        """Rename a file or directory.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path1.startswith('/'):
            path1 = os.path.join(self.get_home_directory(), path1)
        if not path2.startswith('/'):
            path2 = os.path.join(self.get_home_directory(), path2)
        response = self._namenode_request('PUT', path, 'RENAME',
                {'destination': path2})
        content = response.read()
        check_code(response.status, content)
        boolean_json = json.loads(content)
        return boolean_json['boolean']

    def delete(self, path, **args):
        """Make a directory.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('DELETE', path, 'DELETE', args)
        content = response.read()
        check_code(response.status, content)
        boolean_json = json.loads(content)
        return boolean_json['boolean']

    def set_permission(self, path, **args):
        """Set permissions of a file or directory.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('PUT', path, 'SETPERMISSION', args)
        content = response.read()
        check_code(response.status, content)

    def set_permission(self, path, **args):
        """Set permissions of a file or directory.

        If the path is not absolute (starting with '/'), then it's relative to
        the user's home directory.
        """
        if not path.startswith('/'):
            path = os.path.join(self.get_home_directory(), path)
        response = self._namenode_request('PUT', path, 'SETOWNER', args)
        content = response.read()
        check_code(response.status, content)

    ##########################################################################
    # Other

    def _namenode_request(self, method, path, op, args=None, body=None):
        """Send a PUT request to the namenode.

        Returns the HTTPResponse object, which is filelike. Note that this
        response object must be fully read before beginning to read any
        subsequent response.
        """
        request_uri = self._request_uri(path, op, args)
        self._namenode_conn.request(method, request_uri, body)
        response = self._namenode_conn.getresponse()
        return response

    def _datanode_request(self, url, body=None):
        """Send a PUT request to the datanode.

        Returns the HTTPResponse object, which is filelike. Note that this
        response object must be fully read before beginning to read any
        subsequent response.
        """
        host = urlparse.urlparse(url)[1]
        datanode_conn = httplib.HTTPConnection(host)
        # TODO: check whether the datanode is happy with an absolute URL or
        # if it needs a relative url here.
        self._namenode_conn.request(method, url, body)
        response = datanode_conn.getresponse()
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


# Exceptions defined by the webhdfs spec. Note that IllegalArgumentException
# and UnsupportedOperationException are combined.
class IllegalArgumentException(Exception):
    pass

class SecurityException(Exception):
    pass

class IOException(Exception):
    pass

class FileNotFoundException(Exception):
    pass

exceptions = {400: IllegalArgumentException,
        401: SecurityException,
        403: IOException,
        404: FileNotFoundException}

def check_code(code, content, expected_code=httplib.OK):
    """Raise a remote exception if necessary."""
    if code == expected_code:
        return

    try:
        exception_cls = exceptions[code]
    except KeyError:
        raise RuntimeError('Unknown webhdfs error code: %s' % code)

    exception_json = json.loads(content)
    message = exception_json['RemoteException']['message']
    raise exception_cls(message)

# vim: et sw=4 sts=4

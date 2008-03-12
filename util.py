# Mrs
# Copyright 2008 Brigham Young University
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
# Inquiries regarding any further use of the Materials contained on this site,
# please contact the Copyright Licensing Office, Brigham Young University,
# 3760 HBLL, Provo, UT 84602, (801) 422-9339 or 422-3821, e-mail
# copyright@byu.edu.

"""Miscellaneous Helper Functions"""

def try_makedirs(path):
    """Do the equivalent of mkdir -p."""
    import os
    try:
        os.makedirs(path)
    except OSError, e:
        import errno
        if e.errno != errno.EEXIST:
            raise

def rpc_url(urlstring):
    """Tidy a URL to be used to connect to an XML RPC server.

    >>> rpc_url('http://localhost')
    'http://localhost/RPC2'
    >>> rpc_url('http://localhost/')
    'http://localhost/RPC2'
    >>> rpc_url('http://localhost/path/to/xmlrpc')
    'http://localhost/path/to/xmlrpc'
    >>> rpc_url('localhost/path/to/xmlrpc')
    'http://localhost/path/to/xmlrpc'
    >>>
    """
    from urlparse import urlsplit, urlunsplit

    if '://' not in urlstring:
        urlstring = 'http://' + urlstring

    scheme, netloc, path, query, fragment = urlsplit(urlstring)
    if not path and not query and not fragment:
        path = '/RPC2'
    return urlunsplit((scheme, netloc, path, query, fragment))


if __name__ == '__main__':
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

#!/usr/bin/env python
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

# vim: et sw=4 sts=4

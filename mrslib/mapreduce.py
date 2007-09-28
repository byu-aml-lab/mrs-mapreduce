#!/usr/bin/env python

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

from hexfile import HexFile
from textfile import TextFile

# TODO: make it so we can output from map into a different file for each
# reducer.
# TODO: right now we assume that input files are pre-split.
# TODO: start up and close down mappers and reducers.

def mapreduce(mapper, reducer, input_filename):
    """Serial mapreduce for filename input."""
    input_file = TextFile(input_filename)
    intermediate_file = HexFile('intermediate_test.txt', 'w')

    map(mapper, input_file, intermediate_file)
    input_file.close()
    intermediate_file.close()

    # SORT HERE

    intermediate_file = HexFile('intermediate_test.txt')
    output_file = HexFile('output_test.txt', 'w')

    reduce(reducer, intermediate_file, output_file)

    intermediate_file.close()
    output_file.close()

def map(mapper, input_file, output_file):
    """Perform a map from the entries in input_file into output_file.
    """
    while True:
        try:
            input = input_file.next()
            for key, value in mapper(*input):
                output_file.write(key, value)
        except StopIteration:
            return

def grouped_read(input_file):
    """An iterator that yields key-iterator pairs over a sorted input_file.

    This is very similar to itertools.groupby, except that we assume that the
    input_file is sorted, and we assume key-value pairs.
    """
    input = input_file.next()
    next_pair = list(input)

    def subiterator():
        # Closure warning: don't rebind next_pair anywhere in this function
        group_key, value = next_pair

        while True:
            yield value
            try:
                input = input_file.next()
            except StopIteration:
                next_pair[0] = None
                return
            key, value = input
            if key != group_key:
                # A new key has appeared.
                next_pair[:] = key, value
                return

    while next_pair[0] is not None:
        yield next_pair[0], subiterator()
    raise StopIteration


def reduce(reducer, input_file, output_file):
    """Perform a reduce from the entries in input_file into output_file.

    A reducer is an iterator taking a key and an iterator over values for that
    key.  It yields values for that key.  Optionally, the reducer can have a
    switch_keys attribute set to True, in which case it yields key-value
    pairs rather than just values.
    """
    try:
        switch_keys = reducer.switch_keys
    except AttributeError:
        switch_keys = False

    for key, iterator in grouped_read(input_file):
        if switch_keys:
            for new_key, value in reducer(key, iterator):
                output_file.write(new_key, value)
        else:
            for value in reducer(key, iterator):
                output_file.write(key, value)

# vim: et sw=4 sts=4

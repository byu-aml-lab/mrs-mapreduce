# Mrs
# Copyright 2008-2009 Brigham Young University
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

"""Function Registry

The master and slaves need to agree on what functions are available and what
they are called.  A Registry is established before determining the
master/slave roles.  Then everyone can agree on what the names mean.
"""

def func_hash(obj):
    """Hash an object x based on its code.

    If it's a function, hash its code.  Otherwise, hash all of the functions
    in its attributes (excepting attributes beginning with two underscores).
    Note that if the object has no methods or only built-in methods, the hash
    may be 0.

    Hashing of functions:

    >>> def f(x):
    ...   return x**2
    >>> def g(y):
    ...   return y**2
    >>> func_hash(f) == 0
    False
    >>> func_hash(f) == func_hash(g)
    True
    >>>

    Hashing of classes:

    >>> class A(object):
    ...   def __init__(self, x):
    ...     self.x = x
    ...   def f(self):
    ...     return self.x**2
    ...   def g(self):
    ...     return self.x**3
    >>> class B(object):
    ...   def __init__(self):
    ...     self.x = 3
    ...   def f(self):
    ...     return self.x**2
    ...   def g(self):
    ...     return self.x**3
    >>> class C(object):
    ...   def __init__(self, x):
    ...     self.x = x
    ...   def g(self):
    ...     return self.x**3
    >>>
    >>> func_hash(A) == 0
    False
    >>> func_hash(A) == func_hash(B)
    True
    >>> func_hash(A) == func_hash(C)
    False
    >>>

    Hashing of instances:

    >>> a1 = A(4)
    >>> a2 = A(5)
    >>> b = B()
    >>> func_hash(a1) == 0
    False
    >>> func_hash(a1) == func_hash(a2)
    True
    >>> func_hash(a1) == func_hash(b)
    True
    >>>
    """
    try:
        code = obj.func_code.co_code
    except AttributeError:
        attrlist = [getattr(obj, name) for name in dir(obj)
                if not name.startswith('__')]
        codelist = [attr.func_code.co_code for attr in attrlist
                if hasattr(attr, 'func_code')]
        code = ''.join(codelist)
    return str(hash(code))


class Registry(object):
    """Manage a two-way mapping between functions and their names.

    Use this like a dictionary where
    registry['name'] = function

    >>> r = Registry()
    >>> @r.add
    ... def f(x):
    ...   return x
    >>> r['f'] == f
    True
    >>> r.getreverse(f) == 'f'
    True
    >>> def g(x):
    ...   return x
    >>> r['f'] = g
    >>> r['f'] == g
    True
    >>> r.getreverse(g) == 'f'
    True
    >>> f in r.functions
    False
    >>> r['g'] = g
    >>> 'f' in r.names
    False
    >>> r2 = Registry({'f': f, 'g': g})
    >>> r2['f'] == f and r2['g'] == g
    True
    >>> r3 = Registry({'f': f, 'f': g})
    >>> r3['f'] == g
    True
    >>> f in r3.functions
    False
    >>>
    """
    def __init__(self, dictionary=None):
        self.names = {}
        self.functions = {}
        self.hashes = {}
        self.dirty = True

        if dictionary:
            for name, function in dictionary.iteritems():
                self[name] = function

    def __getitem__(self, name):
        return self.names[name]

    def getreverse(self, function):
        return self.functions[function]

    def gethash(self, name):
        return self.hashes[name]

    def __delitem__(self, name):
        function = self.names[name]
        del self.functions[function]
        del self.names[name]

    def delreverse(self, function):
        name = self.functions[function]
        del self.names[name]
        del self.functions[function]

    def __setitem__(self, name, function):
        if name in self.names:
            del self[name]
        try:
            already_defined = (function in self.functions)
        except TypeError:
            # The "function" isn't hashable, so it's not a function.  Give up.
            return
        if already_defined:
            self.delreverse(function)
        self.names[name] = function
        self.functions[function] = name
        self.hashes[name] = func_hash(function)
        self.dirty = True

    def as_name(self, item):
        """Lookup the given string or function and return the string name."""
        if item in self.names:
            return item
        elif item in self.functions:
            return self.functions[item]
        else:
            raise KeyError("Can't find '%s' in registry." % item)

    def add(self, function):
        """Register and return a function.

        Perfect for use as a decorator.
        """
        name = function.func_name
        self[name] = function
        return function

    def __str__(self):
        return str(self.names)

    def source_hash(self):
        """Hash the source file of each function in the registry."""
        if self.dirty:
            filenames = set(self[name].func_globals.get('__file__', '')
                for name in self.names if hasattr(self[name], 'func_globals'))
            hashes = [str(hash(open(filename).read())) for
                filename in sorted(filenames) if filename]
            self.source_hash_cache = ''.join(hashes)
        return self.source_hash_cache

    def reg_hash(self):
        """Hash each function in the registry."""
        if self.dirty:
            self.reg_hash_cache = ''.join(name + self.gethash(name)
                for name in sorted(self.names.keys()))
        return self.reg_hash_cache

    def verify(self, source_hash, reg_hash):
        source_check = (source_hash == self.source_hash())
        reg_check = (reg_hash == self.reg_hash())
        return source_check and reg_check


if __name__ == "__main__":
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

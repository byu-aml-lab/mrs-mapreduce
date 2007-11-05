#!/usr/bin/env python
"""Function Registry

The master and slaves need to agree on what functions are available and what
they are called.  A Registry is established before determining the
master/slave roles.  Then everyone can agree on what the names mean.
"""

class Registry(object):
    """Manage a two-way mapping between functions and their names.

    Use this like a dictionary where
    registry['name'] = function

    >>> r = Registry()
    >>> def f(x):
    ...   return x
    >>> r.add(f)
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

        if dictionary:
            for name, function in dictionary.iteritems():
                self.names[name] = function
                self.functions[function] = name

    def __getitem__(self, name):
        return self.names[name]

    def getreverse(self, function):
        return self.functions[function]

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
        if function in self.functions:
            self.delreverse(function)
        self.names[name] = function
        self.functions[function] = name

    def add(self, function):
        name = function.func_name
        self[name] = function

    def __str__(self):
        return str(self.names)

if __name__ == "__main__":
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

#!/usr/bin/env python
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
        self.main_hash = None

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
        if function in self.functions:
            self.delreverse(function)
        self.names[name] = function
        self.functions[function] = name
        self.hashes[name] = func_hash(function)

    def add(self, function):
        """Register and return a function.

        Perfect for use as a decorator.
        """
        name = function.func_name
        self[name] = function
        return function

    def __str__(self):
        return str(self.names)

    def reg_hash(self):
        return ''.join([name + self.gethash(name)
                for name in sorted(self.names.keys())])

    def verify(self, main_hash, reg_hash):
        if (main_hash == self.main_hash) and (reg_hash == self.reg_hash()):
            return True
        else:
            return False


if __name__ == "__main__":
    import doctest
    doctest.testmod()

# vim: et sw=4 sts=4

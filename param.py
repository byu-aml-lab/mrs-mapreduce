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

"""param.py: Create objects with inheritable keyword parameters/attributes

Anything that subclasses ParamObj will be an object for which _params is
a special directory of Param objects.
"""

import optparse

#TODO: make it so that you can put None in your dictionary to cancel out
# a parameter defined in one of your superclasses.


class ParamError(Exception):
    def __init__(self, clsname, paramname):
        self.clsname = clsname
        self.paramname = paramname

    def __str__(self):
        return 'Class %s has no parameter "%s"' % (self.clsname, self.paramname)


class Param(object):
    """A Parameter with name, default value, and documentation.

    A list of Params is used by a class of type ParamMeta.

    Attributes:
        default: The default value to be used.
        type: The optparse-style type to be used when interpreting command-line
                arguments ('string', 'int', ...).
        doc: Help text.
    """
    def __init__(self, default=None, type='string', doc=None):
        self.default = default
        self.doc = doc
        self.type = type


class _ParamMeta(type):
    """A metaclass that lets you define params.

    When creating a new class of type _ParamMeta, add a dictionary named
    params into the class namespace.  Add Param objects to the dictionary
    with the key being the name of the parameter.  Now, each object of the
    class will have an attribute with the appropriate name.  The value will
    default to the default value in the Param object, but it can be
    overridden by name in __init__.

    Rather than using ParamMeta directly, we recommend that you subclass
    ParamObj, which will allow you to override __init__ as long as you
    call super's __init__.
    """
    def __new__(cls, classname, bases, classdict):

        # Make sure we have a params dict in classdict.
        if '_params' not in classdict:
            classdict['_params'] = {}
        params = classdict['_params']

        # Collect the params from each of the parent classes.
        for base in bases:
            try:
                baseparams = base._params
            except AttributeError:
                # This base class doesn't have a params list.
                continue

            for param_name in baseparams:
                if param_name in params:
                    if params[param_name].doc is None:
                        params[param_name].doc = baseparams[param_name].doc
                else:
                    params[param_name] = baseparams[param_name]

        # Update documentation based on our parameters
        if '__doc__' not in classdict:
            classdict['__doc__'] = '%s -- Class using Params' % classname
        docs = [('%s: %s (default=%s)' % (param_name,
                    params[param_name].doc, params[param_name].default))
                    for param_name in params]
        docs.sort()
        classdict['__doc__'] = classdict['__doc__'] + \
                '\n    '.join(['\n%s Parameters:' % classname] + docs)

        # Write a new __init__ for our classes.  If they write their own
        # __init__, we refuse to overwrite it.
        if '__init__' not in classdict:
            def __init__(self, **kwds):
                for key in kwds:
                    if key not in self._params:
                        raise ParamError(self.__class__.__name__, key)
                for param_name in self._params:
                    if param_name in kwds:
                        value = kwds[param_name]
                    else:
                        value = self._params[param_name].default
                    setattr(self, param_name, value)
            classdict['__init__'] = __init__

        # Create and return the new class
        return type.__new__(cls, classname, bases, classdict)


class ParamObj:
    """An object that treats "_params" specially.

    An object of class ParamObj may contain a dictionary named _params.  This
    dictionary should have string keys and Param values.  For each entry in
    the dictionary, an object attribute is created with the same name as the
    key, and the value and documentation of the attribute are given by the
    arguments to the Param.  Inheritance of _params works right.

    Example:

    >>> m = Rabbit()
    >>> print m.__doc__
    A small rodent, very similar to a hare, which feeds on grass and
        burrows in the earth.
    <BLANKLINE>
    Rabbit Parameters:
        weight: Body Weight (default=42)
    >>> m.weight
    42
    >>> m = Rabbit(weight=12)
    >>> m.weight
    12
    >>>
    """

    __metaclass__ = _ParamMeta


def import_object(name):
    """Imports an object, unlike __import__ which only imports modules.

    Importing a module:
    >>> x = import_object('optparse')
    >>> x == optparse
    True
    >>>

    Importing an attribute in a module:
    >>> x = import_object('optparse.OptionParser')
    >>> x == optparse.OptionParser
    True
    >>>

    Importing a nonexistent module:
    >>> x = import_object('zzzzz')
    Traceback (most recent call last):
        ...
    ImportError: No module named zzzzz
    >>>
    """
    # Note that we don't use fromlist.  As pointed out at
    # http://bugs.python.org/issue2090 and elsewhere, fromlist often does
    # the wrong thing.
    parts = name.split('.')
    try:
        module = __import__(name)
    except ImportError:
        # Check whether name represents an attribute in a module rather than
        # the module itself.
        if len(parts) > 1:
            parent, last = name.rsplit('.', 1)
            module = __import__(parent)
        else:
            raise

    obj = module
    for part in parts[1:]:
        obj = getattr(obj, part)
    return obj


class Option(optparse.Option):
    """Extension of optparse.Option that adds an 'instantiate' action.
    
    >>> parser = optparse.OptionParser(option_class=Option)
    >>> import new
    >>> parser.error = new.instancemethod(test_error, parser)
    >>> option = parser.add_option('--obj', action='instantiate', dest='obj')
    >>>

    >>> opts, args = parser.parse_args(['--obj', 'param.Rabbit',
    ...         '--obj-weight', '17'])
    >>> import param
    >>> isinstance(opts.obj, param.Rabbit)
    True
    >>> opts.obj.weight
    17
    >>>

    >>> opts, args = parser.parse_args(['--obj', 'zzzzz'])
    Traceback (most recent call last):
        ...
    TestFailed: option --obj: No module named zzzzz
    >>>
    """
    ACTIONS = optparse.Option.ACTIONS + ('instantiate',)
    STORE_ACTIONS = optparse.Option.STORE_ACTIONS + ('instantiate',)
    TYPED_ACTIONS = optparse.Option.TYPED_ACTIONS + ('instantiate',)
    ALWAYS_TYPED_ACTIONS = (optparse.Option.ALWAYS_TYPED_ACTIONS
            + ('instantiate',))

    def take_action(self, action, *args):
        if action == 'instantiate':
            self.instantiate(*args)
        else:
            optparse.Option.take_action(self, action, *args)

    def instantiate(self, dest, opt_str, value, values, parser):
        """Instantiates an object and attempts to extend the parser."""
        try:
            cls = import_object(value)
        except ImportError, e:
            message = 'option %s: %s' % (opt_str, str(e))
            raise optparse.OptionValueError(message)

        try:
            obj = cls()
        except Exception, e:
            import traceback
            error = traceback.format_exc()
            message = ('option %s: Could not instantiate.  Traceback:\n%s'
                    % (opt_str, error))
            raise optparse.OptionValueError(message)

        setattr(values, dest, obj)

        for name, param in obj._params.iteritems():
            if self._long_opts:
                prefix = self._long_opts[0][2:]
            else:
                prefix = self._short_opts[0][1]
            option = '--%s-%s' % (prefix, name)
            parser.add_option(option, action='callback',
                    callback=param_callback, callback_args=(obj, name),
                    type=param.type, help=param.doc)


def param_callback(option, opt_str, value, parser, obj, name):
    setattr(obj, name, value)


##############################################################################
# Testing


class TestFailed(Exception):
    """Exception to be raised by an OptionParser when a doctest fails."""


def test_error(self, msg):
    """Instead of printing to stderr and exiting, just raise TestFailed."""
    raise TestFailed(msg)

# A sample ParamObj.
class Rabbit(ParamObj):
    """A small rodent, very similar to a hare, which feeds on grass and
    burrows in the earth.
    """
    _params = dict(weight=Param(default=42, doc='Body Weight', type='int'))

    def __init__(self, **kwds):
        ParamObj.__init__(self, **kwds)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

__all__ = [ParamObj, Param, Option]

# vim: et sw=4 sts=4

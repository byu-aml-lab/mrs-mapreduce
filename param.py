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
        a_b_c: A param with underscores (default='default')
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
    """Imports any object, unlike __import__ which only imports modules.

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


def instantiate(opts, name):
    """Instantiates an object based on settings in a optparse.Values object.

    Suppose that name='abc'.  Then the class specified in opts.abc will be
    instantiated.  The attributes in opts starting with 'abc__' will be used
    to set attributes in the new object.  For example, newobject.hello would
    be set from opts.abc__hello.

    >>> opts = optparse.Values()
    >>> opts.abc = 'param.Rabbit'
    >>> opts.abc__weight = 88
    >>> new = instantiate(opts, 'abc')
    >>> new.weight
    88
    >>> new.__class__.__name__
    'Rabbit'
    >>>
    """
    cls = import_object(getattr(opts, name))
    new = cls()
    prefix = name + '__'
    prefix_len = len(prefix)
    for opts_attr in dir(opts):
        if opts_attr.startswith(prefix):
            new_attr = opts_attr[prefix_len:]
            value = getattr(opts, opts_attr)
            setattr(new, new_attr, value)
    return new


class OptionParser(optparse.OptionParser):
    """Extension of optparse.OptionParser that adds an 'extend' action.

    If you choose action='extend', then the value of the command-line option
    will be imported as a class or module (for example, "mrs.param.Rabbit").
    The class's params will be used to extend the parser, and the full name of
    the class will be saved to the dest.  The attribute "search" can be passed
    to add_option, which specifies a list of modules to be searched when
    importing.  After instantiation, a new command-line option will be created
    for each Param in the class.

    >>> parser = OptionParser()
    >>> import types
    >>> parser.error = types.MethodType(test_error, parser)
    >>> option = parser.add_option('--obj', action='extend', dest='obj')
    >>>

    >>> opts, args = parser.parse_args(['--obj', 'param.Rabbit',
    ...         '--obj-weight', '17'])
    >>> obj = instantiate(opts, 'obj')
    >>> import param
    >>> isinstance(obj, param.Rabbit)
    True
    >>> obj.weight
    17
    >>>

    Note that it automatically converts between hyphens and underscores.
    >>> opts, args = parser.parse_args(['--obj', 'param.Rabbit',
    ...         '--obj-a-b-c', 'hi'])
    >>> obj = instantiate(opts, 'obj')
    >>> obj.a_b_c
    'hi'
    >>>

    Option parsing will fail if an invalid module is specified.
    >>> opts, args = parser.parse_args(['--obj', 'zzzzz'])
    Traceback (most recent call last):
        ...
    TestFailed: option --obj: No module named zzzzz
    >>>
    """

    def __init__(self, **kwds):
        optparse.OptionParser.__init__(self, option_class=_Option, **kwds)

    def get_default_values(self):
        values = optparse.OptionParser.get_default_values(self)
        for option in self._get_all_options():
            if option.action == 'extend':
                opt = option.get_opt_string()
                value = getattr(values, option.dest)
                option.extend(opt, value, values, self)
        return values

    def add_param_object(self, param_obj, prefix=''):
        """Adds a option group for the parameters in a ParamObj.
        
        The given prefix will be prepended to each long option.

        Returns the OptionGroup object associated with this ParamObj.
        """
        title = '%s (%s)' % (param_obj.__name__, prefix)
        subgroup = optparse.OptionGroup(self, title)
        self.add_option_group(subgroup)
        for attr, param in param_obj._params.iteritems():
            name = attr.replace('_', '-')
            if prefix:
                option = '--%s-%s' % (prefix, name)
                dest = '%s__%s' % (prefix, attr)
            else:
                option = '--%s' % name
                dest = name
            doc = '%s (default=%s)' % (param.doc, param.default)
            subgroup.add_option(option, action='store', dest=dest,
                    metavar=attr.upper(), type=param.type, help=doc)
        return subgroup


class _Option(optparse.Option):
    """Extension of optparse.Option that adds an 'extend' action.

    Note that param._Option is automatically used in param.OptionParser.
    """
    ACTIONS = optparse.Option.ACTIONS + ('extend',)
    STORE_ACTIONS = optparse.Option.STORE_ACTIONS + ('extend',)
    TYPED_ACTIONS = optparse.Option.TYPED_ACTIONS + ('extend',)
    ALWAYS_TYPED_ACTIONS = (optparse.Option.ALWAYS_TYPED_ACTIONS + ('extend',))
    ATTRS = optparse.Option.ATTRS + ['search']

    subgroup = None

    def take_action(self, action, dest, *args):
        if action == 'extend':
            self.extend(*args)
        else:
            optparse.Option.take_action(self, action, dest, *args)

    def remove_suboptions(self, parser):
        """Remove any already existing suboptions."""
        if self.subgroup:
            suboptions = list(self.subgroup.option_list)
            for suboption in suboptions:
                parser.remove_option(suboption.get_opt_string())
            parser.option_groups.remove(self.subgroup)
            self.subgroup = None

    def extend(self, opt_str, value, values, parser):
        """Imports a class and attempts to extend the parser."""
        if value is None:
            return

        for base in (self.search or []):
            # Look for modules in the search list.
            try:
                module = '.'.join((base, value))
                paramobj = import_object(module)
                break
            except ImportError, e:
                pass
        else:
            # Since nothing else succeeded, try importing the value directly.
            try:
                paramobj = import_object(value)
            except ImportError, e:
                message = 'option %s: %s' % (opt_str, str(e))
                raise optparse.OptionValueError(message)

        try:
            full_path = '%s.%s' % (paramobj.__module__, paramobj.__name__)
        except AttributeError:
            full_path = paramobj.__name__
        setattr(values, self.dest, full_path)

        self.remove_suboptions(parser)

        if paramobj._params:
            if self._long_opts:
                prefix = self._long_opts[0][2:]
            else:
                prefix = self._short_opts[0][1]
            self.subgroup = parser.add_param_object(paramobj, prefix)


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
    _params = dict(weight=Param(default=42, doc='Body Weight', type='int'),
            a_b_c=Param(default='default', doc='A param with underscores'))

    def __init__(self, **kwds):
        ParamObj.__init__(self, **kwds)


if __name__ == '__main__':
    import doctest
    doctest.testmod()

__all__ = [ParamObj, Param, OptionParser]

# vim: et sw=4 sts=4

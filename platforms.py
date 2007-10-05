#!/usr/bin/env python


def add_implementation(implementation_list):
    def decorator(func):
        implementation_list[func.__name__] = func
        return func
    return decorator

IMPLEMENTATIONS = {}

@add_implementation(IMPLEMENTATIONS)
def posix(options, args):
    pass

@add_implementation(IMPLEMENTATIONS)
def serial(options, args):
    pass


# vim: et sw=4 sts=4

import os
import sys
from ceph_volume import terminal
from functools import wraps


def catches(catch=None, handler=None, exit=True):
    """
    Very simple decorator that tries any of the exception(s) passed in as
    a single exception class or tuple (containing multiple ones) returning the
    exception message and optionally handling the problem if it rises with the
    handler if it is provided.

    So instead of douing something like this::

        def bar():
            try:
                some_call()
                print "Success!"
            except TypeError, exc:
                print "Error while handling some call: %s" % exc
                sys.exit(1)

    You would need to decorate it like this to have the same effect::

        @catches(TypeError)
        def bar():
            some_call()
            print "Success!"

    If multiple exceptions need to be catched they need to be provided as a
    tuple::

        @catches((TypeError, AttributeError))
        def bar():
            some_call()
            print "Success!"
    """
    catch = catch or Exception

    def decorate(f):

        @wraps(f)
        def newfunc(*a, **kw):
            try:
                return f(*a, **kw)
            except catch as e:
                if os.environ.get('CEPH_VOLUME_DEBUG'):
                    raise
                if handler:
                    return handler(e)
                else:
                    sys.stderr.write(make_exception_message(e))
                    if exit:
                        sys.exit(1)
        return newfunc

    return decorate

#
# Decorator helpers
#


def make_exception_message(exc):
    """
    An exception is passed in and this function
    returns the proper string depending on the result
    so it is readable enough.
    """
    if str(exc):
        return '%s %s: %s\n' % (terminal.red_arrow, exc.__class__.__name__, exc)
    else:
        return '%s %s\n' % (terminal.red_arrow, exc.__class__.__name__)

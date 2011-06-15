import contextlib
import sys

@contextlib.contextmanager
def nested(*managers):
    """
    Like contextlib.nested but takes callables returning context
    managers, to avoid the major reason why contextlib.nested was
    deprecated.
    """
    exits = []
    vars = []
    exc = (None, None, None)
    try:
        for mgr_fn in managers:
            mgr = mgr_fn()
            exit = mgr.__exit__
            enter = mgr.__enter__
            vars.append(enter())
            exits.append(exit)
        yield vars
    except:
        exc = sys.exc_info()
    finally:
        while exits:
            exit = exits.pop()
            try:
                if exit(*exc):
                    exc = (None, None, None)
            except:
                exc = sys.exc_info()
        if exc != (None, None, None):
            # Don't rely on sys.exc_info() still containing
            # the right information. Another exception may
            # have been raised and caught by an exit method
            raise exc[0], exc[1], exc[2]

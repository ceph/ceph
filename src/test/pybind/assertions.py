import sys

def single_owner_refcount():
    obj = object()
    return sys.getrefcount(obj)

# Measure the refcount of a single-owner local variable once so tests match
# CPython's bytecode/build-specific refcount behavior (e.g. LOAD_FAST_BORROW
# in Python 3.14 vs. Py_STACKREF_DEBUG builds).
SINGLE_OWNER_REFCOUNT = single_owner_refcount()

def assert_lesser(a, b):
    assert a < b

def assert_equal(a, b):
    assert a == b

def assert_not_equal(a, b):
    assert a != b

def assert_greater(a, b):
    assert a > b

def assert_greater_equal(a, b):
    assert a >= b

def assert_raises(excClass, callableObj, *args, **kwargs):
    """
    Like unittest.TestCase.assertRaises, but returns the exception.
    """
    try:
        callableObj(*args, **kwargs)
    except excClass as e:
        return e
    else:
        if hasattr(excClass, '__name__'):
            excName = excClass.__name__
        else:
            excName = str(excClass)
        raise AssertionError("%s not raised" % excName)

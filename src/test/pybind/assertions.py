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

import random
import requests
import string
import time

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

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        s = s + strpart[:(this_part_size % chunk)]
        yield s
        if (x == size):
            return

def _get_status(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    return status

def _get_status_and_error_code(response):
    status = response['ResponseMetadata']['HTTPStatusCode']
    error_code = response['Error']['Code']
    return status, error_code

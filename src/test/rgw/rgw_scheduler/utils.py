
import platform
import logging
from urllib.parse import urljoin


def assert_py_version():
    major, minor, _ = platform.python_version_tuple()
    assert int(major) >= 3 and int(minor) >=5

def normalise_url_path(baseurl, path):
    # TODO validate the end slash for pure hostname endpoints or signature
    # calculations will fail
    return urljoin(baseurl, path)

def create_buffer(sz):
    data = b"a"*(10**6)
    mv = memoryview(data)
    return mv

def str_to_log_level(log_level='INFO'):
    s = log_level.upper()
    if s == 'DEBUG':
        return logging.DEBUG
    elif s == 'INFO':
        return logging.INFO
    elif s == 'WARNING':
        return logging.WARNING
    elif s == 'ERROR':
        return logging.ERRROR
    else:
        raise ValueError('Invalid Log Level')

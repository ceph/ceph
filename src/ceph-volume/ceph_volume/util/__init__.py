
def as_string(string):
    """
    Ensure that whatever type of string is incoming, it is returned as an
    actual string, versus 'bytes' which Python 3 likes to use.
    """
    if isinstance(string, bytes):
        # we really ignore here if we can't properly decode with utf-8
        return string.decode('utf-8', 'ignore')
    return string

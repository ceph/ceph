import logging
from math import floor

logger = logging.getLogger(__name__)


def as_string(string):
    """
    Ensure that whatever type of string is incoming, it is returned as an
    actual string, versus 'bytes' which Python 3 likes to use.
    """
    if isinstance(string, bytes):
        # we really ignore here if we can't properly decode with utf-8
        return string.decode('utf-8', 'ignore')
    return string


def str_to_int(string, round_down=True):
    """
    Parses a string number into an integer, optionally converting to a float
    and rounding down.
    """
    error_msg = "Unable to convert to integer: '%s'" % str(string)
    try:
        integer = float(string)
    except (TypeError, ValueError):
        logger.exception(error_msg)
        raise RuntimeError(error_msg)

    if round_down:
        integer = floor(integer)
    else:
        integer = round(integer)
    return int(integer)

import logging
from math import floor
from ceph_volume import terminal

try:
    input = raw_input  # pylint: disable=redefined-builtin
except NameError:
    pass

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


def as_bytes(string):
    """
    Ensure that whatever type of string is incoming, it is returned as bytes,
    encoding to utf-8 otherwise
    """
    if isinstance(string, bytes):
        return string
    return string.encode('utf-8', errors='ignore')


def str_to_int(string, round_down=True):
    """
    Parses a string number into an integer, optionally converting to a float
    and rounding down.

    Some LVM values may come with a comma instead of a dot to define decimals.
    This function normalizes a comma into a dot
    """
    error_msg = "Unable to convert to integer: '%s'" % str(string)
    try:
        integer = float(string.replace(',', '.'))
    except AttributeError:
        # this might be a integer already, so try to use it, otherwise raise
        # the original exception
        if isinstance(string, (int, float)):
            integer = string
        else:
            logger.exception(error_msg)
            raise RuntimeError(error_msg)
    except (TypeError, ValueError):
        logger.exception(error_msg)
        raise RuntimeError(error_msg)

    if round_down:
        integer = floor(integer)
    else:
        integer = round(integer)
    return int(integer)


def str_to_bool(val):
    """
    Convert a string representation of truth to True or False

    True values are 'y', 'yes', or ''; case-insensitive
    False values are 'n', or 'no'; case-insensitive
    Raises ValueError if 'val' is anything else.
    """
    true_vals = ['yes', 'y', '']
    false_vals = ['no', 'n']
    try:
        val = val.lower()
    except AttributeError:
        val = str(val).lower()
    if val in true_vals:
        return True
    elif val in false_vals:
        return False
    else:
        raise ValueError("Invalid input value: %s" % val)


def prompt_bool(question, input_=None):
    """
    Interface to prompt a boolean (or boolean-like) response from a user.
    Usually a confirmation.
    """
    input_prompt = input_ or input
    prompt_format = '--> {question} '.format(question=question)
    response = input_prompt(prompt_format)
    try:
        return str_to_bool(response)
    except ValueError:
        terminal.error('Valid true responses are: y, yes, <Enter>')
        terminal.error('Valid false responses are: n, no')
        terminal.error('That response was invalid, please try again')
        return prompt_bool(question, input_=input_prompt)

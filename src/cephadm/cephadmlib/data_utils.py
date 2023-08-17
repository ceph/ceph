# data_utils.py - assorted data management functions


from typing import Dict, Any

from .exceptions import Error


def dict_get(d: Dict, key: str, default: Any = None, require: bool = False) -> Any:
    """
    Helper function to get a key from a dictionary.
    :param d: The dictionary to process.
    :param key: The name of the key to get.
    :param default: The default value in case the key does not
        exist. Default is `None`.
    :param require: Set to `True` if the key is required. An
        exception will be raised if the key does not exist in
        the given dictionary.
    :return: Returns the value of the given key.
    :raises: :exc:`self.Error` if the given key does not exist
        and `require` is set to `True`.
    """
    if require and key not in d.keys():
        raise Error('{} missing from dict'.format(key))
    return d.get(key, default)  # type: ignore


def dict_get_join(d: Dict[str, Any], key: str) -> Any:
    """
    Helper function to get the value of a given key from a dictionary.
    `List` values will be converted to a string by joining them with a
    line break.
    :param d: The dictionary to process.
    :param key: The name of the key to get.
    :return: Returns the value of the given key. If it was a `list`, it
        will be joining with a line break.
    """
    value = d.get(key)
    if isinstance(value, list):
        value = '\n'.join(map(str, value))
    return value


def bytes_to_human(num, mode='decimal'):
    # type: (float, str) -> str
    """Convert a bytes value into it's human-readable form.

    :param num: number, in bytes, to convert
    :param mode: Either decimal (default) or binary to determine divisor
    :returns: string representing the bytes value in a more readable format
    """
    unit_list = ['', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
    divisor = 1000.0
    yotta = 'YB'

    if mode == 'binary':
        unit_list = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB']
        divisor = 1024.0
        yotta = 'YiB'

    for unit in unit_list:
        if abs(num) < divisor:
            return '%3.1f%s' % (num, unit)
        num /= divisor
    return '%.1f%s' % (num, yotta)


def with_units_to_int(v: str) -> int:
    if v.endswith('iB'):
        v = v[:-2]
    elif v.endswith('B'):
        v = v[:-1]
    mult = 1
    if v[-1].upper() == 'K':
        mult = 1024
        v = v[:-1]
    elif v[-1].upper() == 'M':
        mult = 1024 * 1024
        v = v[:-1]
    elif v[-1].upper() == 'G':
        mult = 1024 * 1024 * 1024
        v = v[:-1]
    elif v[-1].upper() == 'T':
        mult = 1024 * 1024 * 1024 * 1024
        v = v[:-1]
    return int(float(v) * mult)

# data_utils.py - assorted data management functions

import datetime
import os
import re
import uuid

from configparser import ConfigParser

from typing import Dict, Any, Optional

from .constants import DATEFMT, DEFAULT_REGISTRY
from .exceptions import Error


def dict_get(
    d: Dict, key: str, default: Any = None, require: bool = False
) -> Any:
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


def read_config(fn):
    # type: (Optional[str]) -> ConfigParser
    cp = ConfigParser()
    if fn:
        cp.read(fn)
    return cp


def try_convert_datetime(s):
    # type: (str) -> Optional[str]
    # This is super irritating because
    #  1) podman and docker use different formats
    #  2) python's strptime can't parse either one
    #
    # I've seen:
    #  docker 18.09.7:  2020-03-03T09:21:43.636153304Z
    #  podman 1.7.0:    2020-03-03T15:52:30.136257504-06:00
    #                   2020-03-03 15:52:30.136257504 -0600 CST
    # (In the podman case, there is a different string format for
    # 'inspect' and 'inspect --format {{.Created}}'!!)

    # In *all* cases, the 9 digit second precision is too much for
    # python's strptime.  Shorten it to 6 digits.
    p = re.compile(r'(\.[\d]{6})[\d]*')
    s = p.sub(r'\1', s)

    # replace trailing Z with -0000, since (on python 3.6.8) it won't parse
    if s and s[-1] == 'Z':
        s = s[:-1] + '-0000'

    # cut off the redundant 'CST' part that strptime can't parse, if
    # present.
    v = s.split(' ')
    s = ' '.join(v[0:3])

    # try parsing with several format strings
    fmts = [
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f %z',
    ]
    for f in fmts:
        try:
            # return timestamp normalized to UTC, rendered as DATEFMT.
            return (
                datetime.datetime.strptime(s, f)
                .astimezone(tz=datetime.timezone.utc)
                .strftime(DATEFMT)
            )
        except ValueError:
            pass
    return None


def is_fsid(s):
    # type: (str) -> bool
    try:
        uuid.UUID(s)
    except ValueError:
        return False
    return True


def normalize_image_digest(digest: str) -> str:
    """
    Normal case:
    >>> normalize_image_digest('ceph/ceph', 'docker.io')
    'docker.io/ceph/ceph'

    No change:
    >>> normalize_image_digest('quay.ceph.io/ceph/ceph', 'docker.io')
    'quay.ceph.io/ceph/ceph'

    >>> normalize_image_digest('docker.io/ubuntu', 'docker.io')
    'docker.io/ubuntu'

    >>> normalize_image_digest('localhost/ceph', 'docker.io')
    'localhost/ceph'
    """
    known_shortnames = [
        'ceph/ceph',
        'ceph/daemon',
        'ceph/daemon-base',
    ]
    for image in known_shortnames:
        if digest.startswith(image):
            return f'{DEFAULT_REGISTRY}/{digest}'
    return digest


def get_legacy_config_fsid(cluster, legacy_dir=None):
    # type: (str, Optional[str]) -> Optional[str]
    config_file = '/etc/ceph/%s.conf' % cluster
    if legacy_dir is not None:
        config_file = os.path.abspath(legacy_dir + config_file)

    if os.path.exists(config_file):
        config = read_config(config_file)
        if config.has_section('global') and config.has_option(
            'global', 'fsid'
        ):
            return config.get('global', 'fsid')
    return None

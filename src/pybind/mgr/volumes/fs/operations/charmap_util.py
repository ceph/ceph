import errno

import cephfs

from ..exception import VolumeException
from ceph.utils import strtobool

_charmap_type = {
    "casesensitive": lambda x: int(strtobool(x)),
    "normalization": lambda x: str(x),
    "encoding": lambda x: str(x),
}

def charmap_set(fs, path, setting, value):
    """
    Set and get a charmap on a directory.
    """

    if setting not in _charmap_type:
        raise VolumeException(-errno.EINVAL, f"charmap setting invalid")

    try:
        value = _charmap_type[setting](value)
    except ValueError:
        raise VolumeException(-errno.EINVAL, f"charmap value wrong type: {setting}")

    try:
        fs.setxattr(path, f"ceph.dir.{setting}", str(value).encode('utf-8'), 0)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

    try:
        return fs.getxattr(path, f"ceph.dir.charmap").decode('utf-8')
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

def charmap_rm(fs, path):
    """
    Remove a charmap on a directory.
    """

    try:
        fs.removexattr(path, "ceph.dir.charmap", 0)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

def charmap_get(fs, path, setting):
    """
    Get a charmap on a directory.
    """

    if setting not in _charmap_type and setting != 'charmap':
        raise VolumeException(-errno.EINVAL, f"charmap setting invalid")

    try:
        return fs.getxattr(path, f"ceph.dir.{setting}").decode('utf-8')
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

import os
import errno
from contextlib import contextmanager

import cephfs

from .snapshot_util import mksnap, rmsnap
from ..fs_util import listdir, get_ancestor_xattr
from ..exception import VolumeException

from .versions import loaded_subvolumes

def create_subvol(fs, vol_spec, group, subvolname, size, isolate_nspace, pool, mode, uid, gid):
    """
    create a subvolume (create a subvolume with the max known version).

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param size: In bytes, or None for no size limit
    :param isolate_nspace: If true, use separate RADOS namespace for this subvolume
    :param pool: the RADOS pool where the data objects of the subvolumes will be stored
    :param mode: the user permissions
    :param uid: the user identifier
    :param gid: the group identifier
    :return: None
    """
    subvolume = loaded_subvolumes.get_subvolume_object_max(fs, vol_spec, group, subvolname)
    subvolume.create(size, isolate_nspace, pool, mode, uid, gid)

def remove_subvol(fs, vol_spec, group, subvolname):
    """
    remove a subvolume.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param subvolname: subvolume name
    :return: None
    """
    with open_subvol(fs, vol_spec, group, subvolname) as subvolume:
        if subvolume.list_snapshots():
            raise VolumeException(-errno.ENOTEMPTY, "subvolume '{0}' has snapshots".format(subvolname))
        subvolume.remove()

@contextmanager
def open_subvol(fs, vol_spec, group, subvolname):
    """
    open a subvolume. This API is to be used as a context manager.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param group: group object for the subvolume
    :param subvolname: subvolume name
    :return: yields a subvolume object (subclass of SubvolumeTemplate)
    """
    subvolume = loaded_subvolumes.get_subvolume_object(fs, vol_spec, group, subvolname)
    subvolume.open()
    yield subvolume

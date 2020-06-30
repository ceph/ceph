import os
import errno
import logging
from contextlib import contextmanager

import cephfs

from .snapshot_util import mksnap, rmsnap
from .pin_util import pin
from .template import GroupTemplate
from ..fs_util import listdir, get_ancestor_xattr
from ..exception import VolumeException

log = logging.getLogger(__name__)

class Group(GroupTemplate):
    # Reserved subvolume group name which we use in paths for subvolumes
    # that are not assigned to a group (i.e. created with group=None)
    NO_GROUP_NAME = "_nogroup"

    def __init__(self, fs, vol_spec, groupname):
        assert groupname != Group.NO_GROUP_NAME
        self.fs = fs
        self.user_id = None
        self.group_id = None
        self.vol_spec = vol_spec
        self.groupname = groupname if groupname else Group.NO_GROUP_NAME

    @property
    def path(self):
        return os.path.join(self.vol_spec.base_dir.encode('utf-8'), self.groupname.encode('utf-8'))

    @property
    def group_name(self):
        return self.groupname

    @property
    def uid(self):
        return self.user_id

    @uid.setter
    def uid(self, val):
        self.user_id = val

    @property
    def gid(self):
        return self.group_id

    @gid.setter
    def gid(self, val):
        self.group_id = val

    def is_default_group(self):
        return self.groupname == Group.NO_GROUP_NAME

    def list_subvolumes(self):
        try:
            return listdir(self.fs, self.path)
        except VolumeException as ve:
            # listing a default group when it's not yet created
            if ve.errno == -errno.ENOENT and self.is_default_group():
                return []
            raise

    def pin(self, pin_type, pin_setting):
        return pin(self.fs, self.path, pin_type, pin_setting)

    def create_snapshot(self, snapname):
        snappath = os.path.join(self.path,
                                self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                                snapname.encode('utf-8'))
        mksnap(self.fs, snappath)

    def remove_snapshot(self, snapname):
        snappath = os.path.join(self.path,
                                self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                                snapname.encode('utf-8'))
        rmsnap(self.fs, snappath)

    def list_snapshots(self):
        try:
            dirpath = os.path.join(self.path,
                                   self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            return listdir(self.fs, dirpath)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return []
            raise

def create_group(fs, vol_spec, groupname, pool, mode, uid, gid):
    """
    create a subvolume group.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param groupname: subvolume group name
    :param pool: the RADOS pool where the data objects of the subvolumes will be stored
    :param mode: the user permissions
    :param uid: the user identifier
    :param gid: the group identifier
    :return: None
    """
    group = Group(fs, vol_spec, groupname)
    path = group.path
    fs.mkdirs(path, mode)
    try:
        if not pool:
            pool = get_ancestor_xattr(fs, path, "ceph.dir.layout.pool")
        try:
            fs.setxattr(path, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL,
                                  "Invalid pool layout '{0}'. It must be a valid data pool".format(pool))
        if uid is None:
            uid = 0
        else:
            try:
                uid = int(uid)
                if uid < 0:
                    raise ValueError
            except ValueError:
                raise VolumeException(-errno.EINVAL, "invalid UID")
        if gid is None:
            gid = 0
        else:
            try:
                gid = int(gid)
                if gid < 0:
                    raise ValueError
            except ValueError:
                raise VolumeException(-errno.EINVAL, "invalid GID")
        fs.chown(path, uid, gid)
    except (cephfs.Error, VolumeException) as e:
        try:
            # cleanup group path on best effort basis
            log.debug("cleaning up subvolume group path: {0}".format(path))
            fs.rmdir(path)
        except cephfs.Error as ce:
            log.debug("failed to clean up subvolume group {0} with path: {1} ({2})".format(groupname, path, ce))
        if isinstance(e, cephfs.Error):
            e = VolumeException(-e.args[0], e.args[1])
        raise e

def remove_group(fs, vol_spec, groupname):
    """
    remove a subvolume group.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param groupname: subvolume group name
    :return: None
    """
    group = Group(fs, vol_spec, groupname)
    try:
        fs.rmdir(group.path)
    except cephfs.Error as e:
        if e.args[0] == errno.ENOENT:
            raise VolumeException(-errno.ENOENT, "subvolume group '{0}' does not exist".format(groupname))
        raise VolumeException(-e.args[0], e.args[1])

@contextmanager
def open_group(fs, vol_spec, groupname):
    """
    open a subvolume group. This API is to be used as a context manager.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param groupname: subvolume group name
    :return: yields a group object (subclass of GroupTemplate)
    """
    group = Group(fs, vol_spec, groupname)
    try:
        st = fs.stat(group.path)
        group.uid = int(st.st_uid)
        group.gid = int(st.st_gid)
    except cephfs.Error as e:
        if e.args[0] == errno.ENOENT:
            if not group.is_default_group():
                raise VolumeException(-errno.ENOENT, "subvolume group '{0}' does not exist".format(groupname))
        else:
            raise VolumeException(-e.args[0], e.args[1])
    yield group

import os
import errno
import logging
from contextlib import contextmanager

import cephfs

from .snapshot_util import mksnap, rmsnap
from .pin_util import pin
from .template import GroupTemplate
from ..fs_util import listdir, listsnaps, get_ancestor_xattr, create_base_dir, has_subdir
from ..exception import VolumeException

log = logging.getLogger(__name__)


class Group(GroupTemplate):
    # Reserved subvolume group name which we use in paths for subvolumes
    # that are not assigned to a group (i.e. created with group=None)
    NO_GROUP_NAME = "_nogroup"

    def __init__(self, fs, vol_spec, groupname):
        if groupname == Group.NO_GROUP_NAME:
            raise VolumeException(-errno.EPERM, "Operation not permitted for group '{0}' as it is an internal group.".format(groupname))
        if groupname in vol_spec.INTERNAL_DIRS:
            raise VolumeException(-errno.EINVAL, "'{0}' is an internal directory and not a valid group name.".format(groupname))
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

    def has_subvolumes(self):
        try:
            return has_subdir(self.fs, self.path)
        except VolumeException as ve:
            # listing a default group when it's not yet created
            if ve.errno == -errno.ENOENT and self.is_default_group():
                return False
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
            return listsnaps(self.fs, self.vol_spec, dirpath, filter_inherited_snaps=True)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return []
            raise

    def info(self):
        st = self.fs.statx(self.path, cephfs.CEPH_STATX_BTIME | cephfs.CEPH_STATX_SIZE
                           | cephfs.CEPH_STATX_UID | cephfs.CEPH_STATX_GID | cephfs.CEPH_STATX_MODE
                           | cephfs.CEPH_STATX_ATIME | cephfs.CEPH_STATX_MTIME | cephfs.CEPH_STATX_CTIME,
                           cephfs.AT_SYMLINK_NOFOLLOW)
        usedbytes = st["size"]
        try:
            nsize = int(self.fs.getxattr(self.path, 'ceph.quota.max_bytes').decode('utf-8'))
        except cephfs.NoData:
            nsize = 0

        try:
            data_pool = self.fs.getxattr(self.path, 'ceph.dir.layout.pool').decode('utf-8')
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        return {'uid': int(st["uid"]),
                'gid': int(st["gid"]),
                'atime': str(st["atime"]),
                'mtime': str(st["mtime"]),
                'ctime': str(st["ctime"]),
                'mode': int(st["mode"]),
                'data_pool': data_pool,
                'created_at': str(st["btime"]),
                'bytes_quota': "infinite" if nsize == 0 else nsize,
                'bytes_used': int(usedbytes),
                'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}

    def resize(self, newsize, noshrink):
        try:
            newsize = int(newsize)
            if newsize <= 0:
                raise VolumeException(-errno.EINVAL, "Invalid subvolume group size")
        except ValueError:
            newsize = newsize.lower()
            if not (newsize == "inf" or newsize == "infinite"):
                raise (VolumeException(-errno.EINVAL, "invalid size option '{0}'".format(newsize)))
            newsize = 0
            noshrink = False

        try:
            maxbytes = int(self.fs.getxattr(self.path, 'ceph.quota.max_bytes').decode('utf-8'))
        except cephfs.NoData:
            maxbytes = 0
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        group_stat = self.fs.stat(self.path)
        if newsize > 0 and newsize < group_stat.st_size:
            if noshrink:
                raise VolumeException(-errno.EINVAL, "Can't resize the subvolume group. The new size"
                                      " '{0}' would be lesser than the current used size '{1}'"
                                      .format(newsize, group_stat.st_size))

        if not newsize == maxbytes:
            try:
                self.fs.setxattr(self.path, 'ceph.quota.max_bytes', str(newsize).encode('utf-8'), 0)
            except cephfs.Error as e:
                raise (VolumeException(-e.args[0],
                                       "Cannot set new size for the subvolume group. '{0}'".format(e.args[1])))
        return newsize, group_stat.st_size

def set_group_attrs(fs, path, attrs):
    # set subvolume group attrs
    # set size
    quota = attrs.get("quota")
    if quota is not None:
        try:
            fs.setxattr(path, 'ceph.quota.max_bytes', str(quota).encode('utf-8'), 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid size specified: '{0}'".format(quota))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    # set pool layout
    pool = attrs.get("data_pool")
    if not pool:
        pool = get_ancestor_xattr(fs, path, "ceph.dir.layout.pool")
    try:
        fs.setxattr(path, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)
    except cephfs.InvalidValue:
        raise VolumeException(-errno.EINVAL,
                              "Invalid pool layout '{0}'. It must be a valid data pool".format(pool))

    # set uid/gid
    uid = attrs.get("uid")
    if uid is None:
        uid = 0
    else:
        try:
            uid = int(uid)
            if uid < 0:
                raise ValueError
        except ValueError:
            raise VolumeException(-errno.EINVAL, "invalid UID")

    gid = attrs.get("gid")
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

    # set mode
    mode = attrs.get("mode", None)
    if mode is not None:
        fs.lchmod(path, mode)

def create_group(fs, vol_spec, groupname, size, pool, mode, uid, gid):
    """
    create a subvolume group.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :param groupname: subvolume group name
    :param size: In bytes, or None for no size limit
    :param pool: the RADOS pool where the data objects of the subvolumes will be stored
    :param mode: the user permissions
    :param uid: the user identifier
    :param gid: the group identifier
    :return: None
    """
    group = Group(fs, vol_spec, groupname)
    path = group.path
    vol_spec_base_dir = group.vol_spec.base_dir.encode('utf-8')

    # create vol_spec base directory with default mode(0o755) if it doesn't exist
    create_base_dir(fs, vol_spec_base_dir, vol_spec.DEFAULT_MODE)
    fs.mkdir(path, mode)
    try:
        attrs = {
            'uid': uid,
            'gid': gid,
            'data_pool': pool,
            'quota': size
        }
        set_group_attrs(fs, path, attrs)
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
        elif e.args[0] == errno.ENOTEMPTY:
            raise VolumeException(-errno.ENOTEMPTY, f"subvolume group {groupname} contains subvolume(s) "
                                  "or retained snapshots of deleted subvolume(s)")
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


@contextmanager
def open_group_unique(fs, vol_spec, groupname, c_group, c_groupname):
    if groupname == c_groupname:
        yield c_group
    else:
        with open_group(fs, vol_spec, groupname) as group:
            yield group

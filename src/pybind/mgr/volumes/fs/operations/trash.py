import os
import uuid
import logging
import errno
from contextlib import contextmanager

import cephfs

from .volume import open_volume_lockless
from .template import GroupTemplate
from .clone_index import PATH_MAX
from ..exception import VolumeException
from ..fs_util import listdir

log = logging.getLogger(__name__)

class Trash(GroupTemplate):
    GROUP_NAME = "_deleting"

    def __init__(self, fs, vol_spec):
        self.fs = fs
        self.vol_spec = vol_spec
        self.groupname = Trash.GROUP_NAME

    @property
    def path(self):
        return os.path.join(self.vol_spec.base_dir.encode('utf-8'), self.groupname.encode('utf-8'))

    @property
    def unique_trash_path(self):
        """
        return a unique trash directory entry path
        """
        return os.path.join(self.path, str(uuid.uuid4()).encode('utf-8'))

    def _get_single_dir_entry(self, exclude_list=[]):
        exclude_list.extend((b".", b".."))
        try:
            with self.fs.opendir(self.path) as d:
                entry = self.fs.readdir(d)
                while entry:
                    if entry.d_name not in exclude_list:
                        return entry.d_name
                    entry = self.fs.readdir(d)
            return None
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def get_trash_entry(self, exclude_list):
        """
        get a trash entry excluding entries provided.

        :praram exclude_list: entries to exclude
        :return: trash entry
        """
        return self._get_single_dir_entry(exclude_list)

    def purge(self, trashpath, should_cancel):
        """
        purge a trash entry.

        :praram trash_entry: the trash entry to purge
        :praram should_cancel: callback to check if the purge should be aborted
        :return: None
        """
        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            try:
                with self.fs.opendir(root_path) as dir_handle:
                    d = self.fs.readdir(dir_handle)
                    while d and not should_cancel():
                        if d.d_name not in (b".", b".."):
                            d_full = os.path.join(root_path, d.d_name)
                            if d.is_dir():
                                rmtree(d_full)
                            else:
                                self.fs.unlink(d_full)
                        d = self.fs.readdir(dir_handle)
            except cephfs.ObjectNotFound:
                return
            except cephfs.Error as e:
                raise VolumeException(-e.args[0], e.args[1])
            # remove the directory only if we were not asked to cancel
            # (else we would fail to remove this anyway)
            if not should_cancel():
                self.fs.rmdir(root_path)

        # catch any unlink errors
        try:
            rmtree(trashpath)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def dump(self, path):
        """
        move an filesystem entity to trash can.

        :praram path: the filesystem path to be moved
        :return: None
        """
        try:
            self.fs.rename(path, self.unique_trash_path)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def link(self, path, bname):
        pth = os.path.join(self.path, bname)
        try:
            self.fs.symlink(path, pth)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def delink(self, bname):
        pth = os.path.join(self.path, bname)
        try:
            self.fs.unlink(pth)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def get_stats(self):
        subvol_count = 0
        file_count = 0
        trash_size = 0

        dir_found = False
        symlink_count = 0

        subvol_count = int(self.fs.getxattr(self.path, 'ceph.dir.subdirs'))
        num_of_entries = int(self.fs.getxattr(self.path, 'ceph.dir.entries'))
        # if all entries are subdirs it implies that there are no symlinks and
        # that implies all subvols are either v1 or v2 but without retained
        # snapshots. in this case, subvols were moved to trash dir and therefore
        # we can fetch all stats on trash dir directly; no need to traverse the
        # file hierarchy under trash dir.
        if subvol_count == num_of_entries:
            file_count = int(self.fs.getxattr(self.path, 'ceph.dir.rfiles'))
            size = int(self.fs.getxattr(self.path, 'ceph.dir.rbytes'))
            return subvol_count, file_count, size

        with self.fs.opendir(self.path) as dir_handle:
            de = self.fs.readdir(dir_handle)
            while de:
                if de.d_name in (b'.', b'..'):
                    de = self.fs.readdir(dir_handle)
                    continue

                try:
                    if de.is_dir():
                        # NOTE: Fetching xattr "rfiles" once on self.path and
                        # then adjusting it is better than running it for every
                        # dir in trash dir. This reduces calls to getxattr to a
                        # great amount, reducing load we place on MGR/MDS. For
                        # this reason value of xattr "rfiles" is not being
                        # collected here and instead it will be collected at a
                        # later point.
                        dir_found = True
                        subvol_count += 1
                    elif de.is_symbol_file():
                        symlink_count += 1

                        subvol_count += 1
                        # every subvolume has a .meta file, and that is not
                        # included in value of xattr rfiles in case where trash
                        # entry is a symlink or, IOW, when subvolume has
                        # retained snapshots. therefore included it now by
                        # incrementing file_count by 1.
                        file_count += 1

                        de_path = os.path.join(self.path, de.d_name)
                        sv_path = self.fs.readlink(de_path, PATH_MAX)
                        file_count += int(self.fs.getxattr(sv_path, 'ceph.dir.rfiles'))
                        trash_size += int(self.fs.getxattr(sv_path, 'ceph.dir.rbytes'))
                    else:
                        log.debug('Trash entry was neither directory nor '
                                  'symlink, this was unexpected. Details: '
                                  f'entry path = {de_path.decode("utf-8")}')
                except cephfs.ObjectNotFound:
                    # we are scanning trash entries while purge threads are
                    # actively deleting them. thus if we get ObjectNotFound it
                    # probably means that it was deleted
                    pass

                de = self.fs.readdir(dir_handle)

        # instead of geting value for xattr for every dir, simply get the value
        # of xattr on trash dir & adjust it. this prevents multiple calls to
        # getxattr().
        if dir_found:
            file_count += int(self.fs.getxattr(self.path, 'ceph.dir.rfiles'))
            # rfiles xattr value counts both regular files as well as symlinks.
            # symlinks shouldn't be included in the count since they represent
            # the subvol dir and subvol_count was incremented to account for it.
            file_count -= symlink_count

            trash_size += int(self.fs.getxattr(self.path, 'ceph.dir.rbytes'))

        return subvol_count, file_count, trash_size


def create_trashcan(fs, vol_spec):
    """
    create a trash can.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :return: None
    """
    trashcan = Trash(fs, vol_spec)
    try:
        fs.mkdirs(trashcan.path, 0o700)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])

@contextmanager
def open_trashcan(fs, vol_spec):
    """
    open a trash can. This API is to be used as a context manager.

    :param fs: ceph filesystem handle
    :param vol_spec: volume specification
    :return: yields a trash can object (subclass of GroupTemplate)
    """
    trashcan = Trash(fs, vol_spec)
    try:
        fs.stat(trashcan.path)
    except cephfs.Error as e:
        raise VolumeException(-e.args[0], e.args[1])
    yield trashcan


def get_trashcan_stats(volclient, volname):
    with open_volume_lockless(volclient, volname) as fs_handle:
        with open_trashcan(fs_handle, volclient.volspec) as trashcan:
            return trashcan.get_stats()


def get_pending_subvol_deletions_count(fs, volspec):
    """
    Get the number of pending subvolumes deletions.
    """
    trashdir = os.path.join(volspec.base_dir, Trash.GROUP_NAME)
    try:
        num_pending_subvol_del = len(listdir(fs, trashdir, filter_entries=None,
                                             filter_files=False))
    except VolumeException as ve:
        if ve.errno == -errno.ENOENT:
            num_pending_subvol_del = 0

    return {'pending_subvolume_deletions': num_pending_subvol_del}

import os
import uuid
import logging
from time import monotonic
from contextlib import contextmanager

import cephfs

from .template import GroupTemplate
from ..exception import VolumeException

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

    def purge(self, trashpath, should_cancel, purge_queue):
        """
        purge a trash entry.

        :praram trash_entry: the trash entry to purge
        :praram should_cancel: callback to check if the purge should be aborted
        :return: None
        """
        def rmtree(root_path):
            nonlocal mpr # type: ignore

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
                                mpr.inc_count()

                        d = self.fs.readdir(dir_handle)
            except cephfs.ObjectNotFound:
                return
            except cephfs.Error as e:
                raise VolumeException(-e.args[0], e.args[1])
            # remove the directory only if we were not asked to cancel
            # (else we would fail to remove this anyway)
            if not should_cancel():
                self.fs.rmdir(root_path)
                mpr.inc_count()

        class MeasurePurgeRate:

            def __init__(self, period, purge_queue):
                # measuring period -- period during which attempt to measure
                # current purge rate is made
                self.period = period
                # this instance variable allows measuring only when measuring
                # period is on
                self.measuring = True

                self.count = 0
                self.rate = 0
                self.time1 = None
                self.time2 = None

            def inc_count(self):
                if not self.measuring:
                    return

                if self.count == 0:
                    self.time1 = monotonic()
                else:
                    assert self.time1
                    self.time2 = monotonic()
                self.count += 1

                if self.time2:
                    time_diff = self.time2 - self.time1
                    if time_diff >= self.period:
                        self.rate = round(self.count / time_diff, 3)
                        log.debug(f'purge rate = {self.rate}')
                        # save the "purge rate" in purge queue object so that
                        # it can be accessed in volume.py
                        purge_queue.purge_rate = self.rate
                        self.reset()

            def pause(self):
                '''
                Stop measuring purge rate.
                '''
                self.measuring = False

            def resume(self):
                '''
                Stop measuring purge rate.
                '''
                self.measuring = True

            def reset(self):
                self.count = 0
                self.rate = 0

                self.time1 = None
                self.time2 = None

        # mpr = measure purge rate. It is an instance of class MeasurePurgeRate
        # (see below) for counting number of calls to unlink() and rmdir() and
        # then compute number of these calls made per second.
        mpr = MeasurePurgeRate(0.001, purge_queue)

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
        try:
            file_count = int(self.fs.getxattr(self.path, 'ceph.dir.rfiles'))
            trash_size = int(self.fs.getxattr(self.path, 'ceph.dir.rbytes'))

            trash_dirs_count = int(self.fs.getxattr(self.path, 'ceph.dir.rsubdirs'))
            # "_deleting" dir was undesirably counted in the variable
            # "trash_dirs_count", reducing count by one now.
            trash_subdirs_count = trash_dirs_count - 1
            # each subvol is placed in under a new UUID dir which is directly
            # located under trash (_deleting) dir. therefore the value of
            # "trash_subdirs_count" is twice of number of subvols.
            #
            # trash_subdirs_count = 1 (one dir per subvol) + 1 (one dir that
            # contains each individual subvol)
            #
            # therefore adjusting this count to get exact  number of subvol
            subvol_count = int(trash_subdirs_count / 2)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

        if file_count:
            return {'subvols_left': subvol_count, 'files_left': file_count,
                    'size_left': trash_size}
        else:
            return {}

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

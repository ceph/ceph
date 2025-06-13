import os
import uuid
import logging
from contextlib import contextmanager
from collections import deque

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

    def purge(self, trashpath, should_cancel):
        """
        Purge a trash entry with non-recursive depth-first approach.
        Non-recursive aspect prevents hitting Python's recursion limit and
        depth-first approach minimizes space complexity to avoid running out
        of memory.

        Besides, repetitive calls to fs.opendir() are prevented by storing
        directory handle on stack and memory consumption is further reduced by
        storing paths relative to trash path instead of absolute paths.

        :praram trash_entry: the trash entry to purge
        :praram should_cancel: callback to check if the purge should be aborted
        :return: None
        """
        log.debug(f'purge(): trashpath = {trashpath}')

        # XXX: add trailing '/' since its absence will produce relative path
        # with leading '/' which will misguide os.path.join() to produce
        # unexpected results.
        #
        # os.path.join('abcd', '123') = '/abcd/123'   --> expected
        # os.path.join('abcd', '/123') = '/123'       --> unexpected
        if trashpath.decode('utf-8')[len(trashpath) - 1] != '/':
            trashpath += b'/'

        try:
            # each stack member is tuple containing path to a directory and
            # handle for that directory.
            #
            # The path here is a relative path (relative to trash path) instead
            # of absolute path. This is to minimize the memory consumption and
            # thereby also minimize the chance of running out of memory.
            #
            # The directory handle is also stored on stack to avoid repetitive
            # calls to fs.opendir().
            stack = deque([(b'', self.fs.opendir(trashpath)), ])
            while stack and not should_cancel():
                last_index = len(stack) - 1
                curr_dir_path = os.path.join(trashpath, stack[last_index][0])
                curr_dir_handle = stack[last_index][1]

                # assuming True for now, if it's not empty upcoming loop will
                # set it to False.
                is_curr_dir_empty = True

                # de = directory entry
                de = self.fs.readdir(curr_dir_handle)
                while de and not should_cancel():
                    if de.d_name in (b'.', b'..'):
                        de = self.fs.readdir(curr_dir_handle)
                        continue

                    is_curr_dir_empty = False
                    de_path = os.path.join(curr_dir_path, de.d_name)
                    if de.is_dir():
                        try:
                            self.fs.rmdir(de_path)
                        except cephfs.ObjectNotEmpty:
                            de_rel_path = de_path.replace(trashpath, b'')
                            stack.append((de_rel_path, self.fs.opendir(de_path)))
                            break
                    else:
                        self.fs.unlink(de_path)

                    de = self.fs.readdir(curr_dir_handle)

                if is_curr_dir_empty:
                    stack.pop()
                else:
                    curr_dir_handle.rewinddir()
                    continue

            self.fs.rmdir(trashpath)
        except cephfs.ObjectNotFound:
            return
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

"""
Copyright (C) 2019 Red Hat, Inc.

LGPL2.1.  See file COPYING.
"""

import logging
import os
import errno

import cephfs

from .subvolspec import SubvolumeSpec
from .exception import VolumeException

log = logging.getLogger(__name__)

class SubVolume(object):
    """
    Combine libcephfs and librados interfaces to implement a
    'Subvolume' concept implemented as a cephfs directory.

    Additionally, subvolumes may be in a 'Group'.  Conveniently,
    subvolumes are a lot like manila shares, and groups are a lot
    like manila consistency groups.

    Refer to subvolumes with SubvolumePath, which specifies the
    subvolume and group IDs (both strings).  The group ID may
    be None.

    In general, functions in this class are allowed raise rados.Error
    or cephfs.Error exceptions in unexpected situations.
    """


    def __init__(self, mgr, fs_handle):
        self.fs = fs_handle
        self.rados = mgr.rados

    def _get_single_dir_entry(self, dir_path, exclude=[]):
        """
        Return a directory entry in a given directory excluding passed
        in entries.
        """
        exclude.extend((b".", b".."))
        try:
            with self.fs.opendir(dir_path) as d:
                entry = self.fs.readdir(d)
                while entry:
                    if entry.d_name not in exclude and entry.is_dir():
                        return entry.d_name
                    entry = self.fs.readdir(d)
            return None
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])


    ### basic subvolume operations

    def create_subvolume(self, spec, size=None, namespace_isolated=True, mode=0o755, pool=None):
        """
        Set up metadata, pools and auth for a subvolume.

        This function is idempotent.  It is safe to call this again
        for an already-created subvolume, even if it is in use.

        :param spec: subvolume path specification
        :param size: In bytes, or None for no size limit
        :param namespace_isolated: If true, use separate RADOS namespace for this subvolume
        :param pool: the RADOS pool where the data objects of the subvolumes will be stored
        :return: None
        """
        subvolpath = spec.subvolume_path
        log.info("creating subvolume with path: {0}".format(subvolpath))

        self.fs.mkdirs(subvolpath, mode)

        try:
            if size is not None:
                try:
                    self.fs.setxattr(subvolpath, 'ceph.quota.max_bytes', str(size).encode('utf-8'), 0)
                except cephfs.InvalidValue as e:
                    raise VolumeException(-errno.EINVAL, "Invalid size: '{0}'".format(size))
            if pool:
                try:
                    self.fs.setxattr(subvolpath, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)
                except cephfs.InvalidValue:
                    raise VolumeException(-errno.EINVAL,
                                          "Invalid pool layout '{0}'. It must be a valid data pool".format(pool))

            xattr_key = xattr_val = None
            if namespace_isolated:
                # enforce security isolation, use separate namespace for this subvolume
                xattr_key = 'ceph.dir.layout.pool_namespace'
                xattr_val = spec.fs_namespace
            elif not pool:
                # If subvolume's namespace layout is not set, then the subvolume's pool
                # layout remains unset and will undesirably change with ancestor's
                # pool layout changes.
                xattr_key = 'ceph.dir.layout.pool'
                xattr_val = self._get_ancestor_xattr(subvolpath, "ceph.dir.layout.pool")
            # TODO: handle error...
            self.fs.setxattr(subvolpath, xattr_key, xattr_val.encode('utf-8'), 0)
        except Exception as e:
            try:
                # cleanup subvol path on best effort basis
                log.debug("cleaning up subvolume with path: {0}".format(subvolpath))
                self.fs.rmdir(subvolpath)
            except Exception:
                log.debug("failed to clean up subvolume with path: {0}".format(subvolpath))
                pass
            finally:
                raise e

    def remove_subvolume(self, spec, force):
        """
        Make a subvolume inaccessible to guests.  This function is idempotent.
        This is the fast part of tearing down a subvolume. The subvolume will
        get purged in the background.

        :param spec: subvolume path specification
        :param force: flag to ignore non-existent path (never raise exception)
        :return: None
        """

        subvolpath = spec.subvolume_path
        log.info("deleting subvolume with path: {0}".format(subvolpath))

        # Create the trash directory if it doesn't already exist
        trashdir = spec.trash_dir
        self.fs.mkdirs(trashdir, 0o700)

        # mangle the trash directroy entry to a random string so that subsequent
        # subvolume create and delete with same name moves the subvolume directory
        # to a unique trash dir (else, rename() could fail if the trash dir exist).
        trashpath = spec.unique_trash_path
        try:
            self.fs.rename(subvolpath, trashpath)
        except cephfs.ObjectNotFound:
            if not force:
                raise VolumeException(
                    -errno.ENOENT, "Subvolume '{0}' not found, cannot remove it".format(spec.subvolume_id))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def resize_subvolume(self, subvolpath, newsize, noshrink):
        """
        :param subvolpath: subvolume path
        :param newsize: new size In bytes
        :return: new quota size and used bytes as a tuple
        """
        if newsize <= 0:
            raise VolumeException(-errno.EINVAL, "Provide a valid size")

        try:
            maxbytes = int(self.fs.getxattr(subvolpath, 'ceph.quota.max_bytes').decode('utf-8'))
        except cephfs.NoData:
            maxbytes = 0

        subvolstat = self.fs.stat(subvolpath)
        if newsize > 0 and newsize < subvolstat.st_size:
            if noshrink:
                raise VolumeException(-errno.EINVAL, "Can't resize the subvolume. The new size '{0}' would be lesser than the current "
                                      "used size '{1}'".format(newsize, subvolstat.st_size))

        if newsize == maxbytes:
            return newsize, subvolstat.st_size

        try:
            self.fs.setxattr(subvolpath, 'ceph.quota.max_bytes', str(newsize).encode('utf-8'), 0)
        except Exception as e:
            raise VolumeException(-e.args[0], "Cannot set new size for the subvolume. '{0}'".format(e.args[1]))
        return newsize, subvolstat.st_size

    def resize_infinite(self, subvolpath, newsize):
        """
        :param subvolpath: the subvolume path
        :param newsize: the string inf
        :return: new quota size and used bytes as a tuple
        """

        if not (newsize == "inf" or newsize == "infinite"):
            raise VolumeException(-errno.EINVAL, "Invalid parameter '{0}'".format(newsize))

        subvolstat = self.fs.stat(subvolpath)
        size = 0
        try:
            self.fs.setxattr(subvolpath, 'ceph.quota.max_bytes', str(size).encode('utf-8'), 0)
        except Exception as e:
            raise VolumeException(-errno.ENOENT, "Cannot resize the subvolume to infinite size. '{0}'".format(e.args[1]))
        return size, subvolstat.st_size

    def purge_subvolume(self, spec, should_cancel):
        """
        Finish clearing up a subvolume from the trash directory.
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

        trashpath = spec.trash_path
        # catch any unlink errors
        try:
            rmtree(trashpath)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def get_subvolume_path(self, spec):
        path = spec.subvolume_path
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])
        return path

    def get_dir_entries(self, path):
        """
        Get the directory names in a given path
        :param path: the given path
        :return: the list of directory names
        """
        dirs = []
        try:
            with self.fs.opendir(path) as dir_handle:
                d = self.fs.readdir(dir_handle)
                while d:
                    if (d.d_name not in (b".", b"..")) and d.is_dir():
                        dirs.append(d.d_name)
                    d = self.fs.readdir(dir_handle)
        except cephfs.ObjectNotFound:
            # When the given path is not found, we just return an empty list
            return []
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])
        return dirs

    ### group operations

    def create_group(self, spec, mode=0o755, pool=None):
        path = spec.group_path
        self.fs.mkdirs(path, mode)
        try:
            if not pool:
                pool = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
            try:
                self.fs.setxattr(path, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)
            except cephfs.InvalidValue:
                raise VolumeException(-errno.EINVAL,
                                      "Invalid pool layout '{0}'. It must be a valid data pool".format(pool))
        except Exception as e:
            try:
                # cleanup group path on best effort basis
                log.debug("cleaning up subvolumegroup with path: {0}".format(path))
                self.fs.rmdir(path)
            except Exception:
                log.debug("failed to clean up subvolumegroup with path: {0}".format(path))
                pass
            finally:
                raise e

    def remove_group(self, spec, force):
        path = spec.group_path
        try:
            self.fs.rmdir(path)
        except cephfs.ObjectNotFound:
            if not force:
                raise VolumeException(-errno.ENOENT, "Subvolume group '{0}' not found".format(spec.group_id))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def get_group_path(self, spec):
        path = spec.group_path
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        return path

    def _get_ancestor_xattr(self, path, attr):
        """
        Helper for reading layout information: if this xattr is missing
        on the requested path, keep checking parents until we find it.
        """
        try:
            return self.fs.getxattr(path, attr).decode('utf-8')
        except cephfs.NoData:
            if path == "/":
                raise
            else:
                return self._get_ancestor_xattr(os.path.split(path)[0], attr)

    ### snapshot operations

    def _snapshot_create(self, snappath, mode=0o755):
        """
        Create a snapshot, or do nothing if it already exists.
        """
        try:
            self.fs.stat(snappath)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(snappath, mode)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])
        else:
            log.warn("Snapshot '{0}' already exists".format(snappath))

    def _snapshot_delete(self, snappath, force):
        """
        Remove a snapshot, or do nothing if it doesn't exist.
        """
        try:
            self.fs.stat(snappath)
            self.fs.rmdir(snappath)
        except cephfs.ObjectNotFound:
            if not force:
                raise VolumeException(-errno.ENOENT, "Snapshot '{0}' not found, cannot remove it".format(snappath))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def create_subvolume_snapshot(self, spec, snapname, mode=0o755):
        snappath = spec.make_subvol_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_create(snappath, mode)

    def remove_subvolume_snapshot(self, spec, snapname, force):
        snappath = spec.make_subvol_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_delete(snappath, force)

    def create_group_snapshot(self, spec, snapname, mode=0o755):
        snappath = spec.make_group_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        self._snapshot_create(snappath, mode)

    def remove_group_snapshot(self, spec, snapname, force):
        snappath = spec.make_group_snap_path(self.rados.conf_get('client_snapdir'), snapname)
        return self._snapshot_delete(snappath, force)

    def get_trash_entry(self, spec, exclude):
        try:
            trashdir = spec.trash_dir
            return self._get_single_dir_entry(trashdir, exclude)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                # trash dir does not exist yet, signal success
                return None
            raise

    ### context manager routines

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

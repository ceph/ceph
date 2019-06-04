"""
Copyright (C) 2019 Red Hat, Inc.

LGPL2.1.  See file COPYING.
"""

import errno
import logging
import os

import cephfs
import rados


log = logging.getLogger(__name__)

# Reserved subvolume group name which we use in paths for subvolumes
# that are not assigned to a group (i.e. created with group=None)
NO_GROUP_NAME = "_nogroup"


class SubvolumePath(object):
    """
    Identify a subvolume's path as group->subvolume
    The Subvolume ID is a unique identifier, but this is a much more
    helpful thing to pass around.
    """
    def __init__(self, group_id, subvolume_id):
        self.group_id = group_id
        self.subvolume_id = subvolume_id
        assert self.group_id != NO_GROUP_NAME
        assert self.subvolume_id != "" and self.subvolume_id is not None

    def __str__(self):
        return "{0}/{1}".format(self.group_id, self.subvolume_id)


class SubvolumeClient(object):
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

    # Where shall we create our subvolumes?
    DEFAULT_SUBVOL_PREFIX = "/volumes"
    DEFAULT_NS_PREFIX = "fsvolumens_"

    def __init__(self, mgr, subvolume_prefix=None, pool_ns_prefix=None, fs_name=None):
        self.fs = None
        self.fs_name = fs_name
        self.connected = False

        self.rados = mgr.rados

        self.subvolume_prefix = subvolume_prefix if subvolume_prefix else self.DEFAULT_SUBVOL_PREFIX
        self.pool_ns_prefix = pool_ns_prefix if pool_ns_prefix else self.DEFAULT_NS_PREFIX

    def _subvolume_path(self, subvolume_path):
        """
        Determine the path within CephFS where this subvolume will live
        :return: absolute path (string)
        """
        return os.path.join(
            self.subvolume_prefix,
            subvolume_path.group_id if subvolume_path.group_id is not None else NO_GROUP_NAME,
            subvolume_path.subvolume_id)

    def _group_path(self, group_id):
        """
        Determine the path within CephFS where this subvolume group will live
        :return: absolute path (string)
        """
        if group_id is None:
            raise ValueError("group_id may not be None")

        return os.path.join(
            self.subvolume_prefix,
            group_id
        )

    def connect(self):
        log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.rados)
        log.debug("CephFS initializing...")
        self.fs.init()
        log.debug("CephFS mounting...")
        self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
        log.debug("Connection to cephfs complete")

    def disconnect(self):
        log.info("disconnect")
        if self.fs:
            log.debug("Disconnecting cephfs...")
            self.fs.shutdown()
            self.fs = None
            log.debug("Disconnecting cephfs complete")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def _mkdir_p(self, path, mode=0o755):
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            pass
        else:
            return

        parts = path.split(os.path.sep)

        for i in range(1, len(parts) + 1):
            subpath = os.path.join(*parts[0:i])
            try:
                self.fs.stat(subpath)
            except cephfs.ObjectNotFound:
                self.fs.mkdir(subpath, mode)

    def create_group(self, group_id, mode=0o755):
        path = self._group_path(group_id)
        self._mkdir_p(path, mode)

    def delete_group(self, group_id):
        path = self._group_path(group_id)
        self.fs.rmdir(path)

    def create_subvolume(self, subvolume_path, size=None, namespace_isolated=True, mode=0o755):
        """
        Set up metadata, pools and auth for a subvolume.

        This function is idempotent.  It is safe to call this again
        for an already-created subvolume, even if it is in use.

        :param subvolume_path: SubvolumePath instance
        :param size: In bytes, or None for no size limit
        :param namespace_isolated: If true, use separate RADOS namespace for this subvolume
        :return: None
        """
        path = self._subvolume_path(subvolume_path)
        log.info("creating subvolume with path: {0}".format(path))

        self._mkdir_p(path, mode)

        if size is not None:
            self.fs.setxattr(path, 'ceph.quota.max_bytes', str(size).encode('utf-8'), 0)

        # enforce security isolation, use separate namespace for this subvolume
        if namespace_isolated:
            namespace = "{0}{1}".format(self.pool_ns_prefix, subvolume_path.subvolume_id)
            log.info("creating subvolume with path: {0}, using rados namespace {1} to isolate data.".format(subvolume_path, namespace))
            self.fs.setxattr(path, 'ceph.dir.layout.pool_namespace',
                             namespace.encode('utf-8'), 0)
        else:
            # If subvolume's namespace layout is not set, then the subvolume's pool
            # layout remains unset and will undesirably change with ancestor's
            # pool layout changes.
            pool_name = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
            self.fs.setxattr(path, 'ceph.dir.layout.pool',
                             pool_name.encode('utf-8'), 0)

    def delete_subvolume(self, subvolume_path):
        """
        Make a subvolume inaccessible to guests.  This function is idempotent.
        This is the fast part of tearing down a subvolume: you must also later
        call purge_subvolume, which is the slow part.

        :param subvolume_path: Same identifier used in create_subvolume
        :return: None
        """

        path = self._subvolume_path(subvolume_path)
        log.info("deleting subvolume with path: {0}".format(path))

        # Create the trash folder if it doesn't already exist
        trash = os.path.join(self.subvolume_prefix, "_deleting")
        self._mkdir_p(trash)

        # We'll move it to here
        trashed_subvolume = os.path.join(trash, subvolume_path.subvolume_id)

        # Move the subvolume to the trash folder
        self.fs.rename(path, trashed_subvolume)

    def purge_subvolume(self, subvolume_path):
        """
        Finish clearing up a subvolume that was previously passed to delete_subvolume.  This
        function is idempotent.
        """

        trash = os.path.join(self.subvolume_prefix, "_deleting")
        trashed_subvolume = os.path.join(trash, subvolume_path.subvolume_id)

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            try:
                dir_handle = self.fs.opendir(root_path)
            except cephfs.ObjectNotFound:
                return
            d = self.fs.readdir(dir_handle)
            while d:
                d_name = d.d_name.decode('utf-8')
                if d_name not in [".", ".."]:
                    # Do not use os.path.join because it is sensitive
                    # to string encoding, we just pass through dnames
                    # as byte arrays
                    d_full = "{0}/{1}".format(root_path, d_name)
                    if d.is_dir():
                        rmtree(d_full)
                    else:
                        self.fs.unlink(d_full)

                d = self.fs.readdir(dir_handle)
            self.fs.closedir(dir_handle)

            self.fs.rmdir(root_path)

        rmtree(trashed_subvolume)


    def _get_ancestor_xattr(self, path, attr):
        """
        Helper for reading layout information: if this xattr is missing
        on the requested path, keep checking parents until we find it.
        """
        try:
            result = self.fs.getxattr(path, attr).decode('utf-8')
            if result == "":
                # Annoying!  cephfs gives us empty instead of an error when attr not found
                raise cephfs.NoData()
            else:
                return result
        except cephfs.NoData:
            if path == "/":
                raise
            else:
                return self._get_ancestor_xattr(os.path.split(path)[0], attr)

    def get_group_path(self, group_id):
        path = self._group_path(group_id)
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        return path

    def get_subvolume_path(self, subvolume_path):
        path = self._subvolume_path(subvolume_path)
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        return path

    def _snapshot_path(self, dir_path, snapshot_name):
        return os.path.join(
            dir_path, self.rados.conf_get('client_snapdir'), snapshot_name
        )

    def _snapshot_create(self, dir_path, snapshot_name, mode=0o755):
        """
        Create a snapshot, or do nothing if it already exists.
        """
        snapshot_path = self._snapshot_path(dir_path, snapshot_name)
        try:
            self.fs.stat(snapshot_path)
        except cephfs.ObjectNotFound:
            self.fs.mkdir(snapshot_path, mode)
        else:
            log.warn("Snapshot '{0}' already exists".format(snapshot_name))


    def _snapshot_delete(self, dir_path, snapshot_name):
        """
        Remove a snapshot, or do nothing if it doesn't exist.
        """
        snapshot_path = self._snapshot_path(dir_path, snapshot_name)
        self.fs.stat(snapshot_path)
        self.fs.rmdir(snapshot_path)

    def create_subvolume_snapshot(self, subvolume_path, snapshot_name, mode=0o755):
        return self._snapshot_create(self._subvolume_path(subvolume_path), snapshot_name, mode)

    def delete_subvolume_snapshot(self, subvolume_path, snapshot_name):
        return self._snapshot_delete(self._subvolume_path(subvolume_path), snapshot_name)

    def create_group_snapshot(self, group_id, snapshot_name, mode=0o755):
        return self._snapshot_create(self._group_path(group_id), snapshot_name, mode)

    def delete_group_snapshot(self, group_id, snapshot_name):
        return self._snapshot_delete(self._group_path(group_id), snapshot_name)

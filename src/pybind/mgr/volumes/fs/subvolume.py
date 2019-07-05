"""
Copyright (C) 2019 Red Hat, Inc.

LGPL2.1.  See file COPYING.
"""

import logging
import os
import json
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


    def __init__(self, mgr, fs_name=None):
        self.fs = None
        self.fs_name = fs_name
        self.connected = False
        self.mgr = mgr
        self.rados = mgr.rados

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
            except cephfs.Error as e:
                raise VolumeException(e.args[0], e.args[1])

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

        self._mkdir_p(subvolpath, mode)

        if size is not None:
            self.fs.setxattr(subvolpath, 'ceph.quota.max_bytes', str(size).encode('utf-8'), 0)

        if pool:
            self.fs.setxattr(subvolpath, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)

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

    def remove_subvolume(self, spec, force):
        """
        Make a subvolume inaccessible to guests.  This function is idempotent.
        This is the fast part of tearing down a subvolume: you must also later
        call purge_subvolume, which is the slow part.

        :param spec: subvolume path specification
        :param force: flag to ignore non-existent path (never raise exception)
        :return: None
        """

        subvolpath = spec.subvolume_path
        log.info("deleting subvolume with path: {0}".format(subvolpath))

        # Create the trash directory if it doesn't already exist
        trashdir = spec.trash_dir
        self._mkdir_p(trashdir)

        trashpath = spec.trash_path
        try:
            self.fs.rename(subvolpath, trashpath)
        except cephfs.ObjectNotFound:
            if not force:
                raise VolumeException(
                    -errno.ENOENT, "Subvolume '{0}' not found, cannot remove it".format(spec.subvolume_id))
        except cephfs.Error as e:
            raise VolumeException(e.args[0], e.args[1])

    def purge_subvolume(self, spec):
        """
        Finish clearing up a subvolume that was previously passed to delete_subvolume.  This
        function is idempotent.
        """

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            try:
                dir_handle = self.fs.opendir(root_path)
            except cephfs.ObjectNotFound:
                return
            except cephfs.Error as e:
                raise VolumeException(e.args[0], e.args[1])
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

        trashpath = spec.trash_path
        rmtree(trashpath)

    def get_subvolume_path(self, spec):
        path = spec.subvolume_path
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            return None
        except cephfs.Error as e:
            raise VolumeException(e.args[0], e.args[1])
        return path

    def authorize_subvolume(self, spec, auth_id, access_level):
        path = spec.subvolume_path
        log.debug("Authorizing Ceph id '{0}' for path '{1}'".format(auth_id, path))

        # First I need to work out what the data pool is for this share:
        # read the layout
        pool = self.fs.getxattr(path, "ceph.dir.layout.pool").decode('utf-8')
        namespace = self.fs.getxattr(path, "ceph.dir.layout.pool_namespace").decode('utf-8')

        # Now construct auth capabilities that give the guest just enough
        # permissions to access the share
        client_entity = "client.{0}".format(auth_id)
        want_mds_cap = "allow {0} path={1}".format(access_level, path)
        want_osd_cap = "allow {0} pool={1} namespace={2}".format(access_level, pool, namespace)

        ret, out, err = self.mgr.mon_command({
            "prefix": "auth get",
            "entity": client_entity,
            "format": "json"})

        if ret == -errno.ENOENT:
            ret, out, err = self.mgr.mon_command({
                "prefix": "auth get-or-create",
                "entity": client_entity,
                "caps": ["mds", want_mds_cap,"osd", want_osd_cap,"mon", 'allow r'],
                "format": "json"})
        else:
            cap = json.loads(out)[0]
            want_access_level = access_level

            # Construct auth caps that if present might conflict with the desired
            # auth caps.
            unwanted_access_level = 'r' if want_access_level is 'rw' else 'rw'
            unwanted_mds_cap = 'allow {0} path={1}'.format(unwanted_access_level, path)
            unwanted_osd_cap = 'allow {0} pool={1} namespace={2}'.format(
                    unwanted_access_level, pool, namespace)

            def cap_update(
                    orig_mds_caps, orig_osd_caps, want_mds_cap,
                    want_osd_cap, unwanted_mds_cap, unwanted_osd_cap):

                if not orig_mds_caps:
                    return want_mds_cap, want_osd_cap

                mds_cap_tokens = orig_mds_caps.split(",")
                osd_cap_tokens = orig_osd_caps.split(",")

                if want_mds_cap in mds_cap_tokens:
                    return orig_mds_caps, orig_osd_caps

                if unwanted_mds_cap in mds_cap_tokens:
                    mds_cap_tokens.remove(unwanted_mds_cap)
                    osd_cap_tokens.remove(unwanted_osd_cap)

                mds_cap_tokens.append(want_mds_cap)
                osd_cap_tokens.append(want_osd_cap)

                return ",".join(mds_cap_tokens), ",".join(osd_cap_tokens)

            orig_mds_caps = cap['caps'].get('mds', "")
            orig_osd_caps = cap['caps'].get('osd', "")

            mds_cap_str, osd_cap_str = cap_update(
                orig_mds_caps, orig_osd_caps, want_mds_cap, want_osd_cap,
                unwanted_mds_cap, unwanted_osd_cap)

            self.mgr.mon_command(
                {
                    "prefix": "auth caps",
                    'entity': client_entity,
                    'caps': [
                        'mds', mds_cap_str,
                        'osd', osd_cap_str,
                        'mon', cap['caps'].get('mon', 'allow r')],
                })
            ret, out, err = self.mgr.mon_command(
                {
                    'prefix': 'auth get',
                    'entity': client_entity,
                    'format': 'json'
                })

        # Result expected like this:
        # [
        #     {
        #         "entity": "client.foobar",
        #         "key": "AQBY0\/pViX\/wBBAAUpPs9swy7rey1qPhzmDVGQ==",
        #         "caps": {
        #             "mds": "allow *",
        #             "mon": "allow *"
        #         }
        #     }
        # ]

        caps = json.loads(out)
        assert len(caps) == 1
        assert caps[0]['entity'] == client_entity
        return caps[0]['key']

    def deauthorize_subvolume(self, spec, auth_id):
        """
        The volume must still exist.
        """
        client_entity = "client.{0}".format(auth_id)
        path = spec.subvolume_path
        pool_name = self.fs.getxattr(path, "ceph.dir.layout.pool").decode('utf-8')
        namespace = self.fs.getxattr(path, "ceph.dir.layout.pool_namespace").decode('utf-8')

        # The auth_id might have read-only or read-write mount access for the
        # subvolume path.
        access_levels = ('r', 'rw')
        want_mds_caps = ['allow {0} path={1}'.format(access_level, path)
                         for access_level in access_levels]
        want_osd_caps = ['allow {0} pool={1} namespace={2}'.format(access_level, pool_name, namespace)
                         for access_level in access_levels]


        ret, out, err = self.mgr.mon_command({
            "prefix": "auth get",
            "entity": client_entity,
            "format": "json",
        })

        if ret == -errno.ENOENT:
            # Already gone, great.
            return

        def cap_remove(orig_mds_caps, orig_osd_caps, want_mds_caps, want_osd_caps):
            mds_cap_tokens = orig_mds_caps.split(",")
            osd_cap_tokens = orig_osd_caps.split(",")

            for want_mds_cap, want_osd_cap in zip(want_mds_caps, want_osd_caps):
                if want_mds_cap in mds_cap_tokens:
                    mds_cap_tokens.remove(want_mds_cap)
                    osd_cap_tokens.remove(want_osd_cap)
                    break

            return ",".join(mds_cap_tokens), ",".join(osd_cap_tokens)

        cap = json.loads(out)[0]
        orig_mds_caps = cap['caps'].get('mds', "")
        orig_osd_caps = cap['caps'].get('osd', "")
        mds_cap_str, osd_cap_str = cap_remove(orig_mds_caps, orig_osd_caps,
                                              want_mds_caps, want_osd_caps)

        if not mds_cap_str:
            self.mgr.mon_command(
                {
                    'prefix': 'auth rm',
                    'entity': client_entity
                })
        else:
            self.mgr.mon_command(
                {
                    "prefix": "auth caps",
                    'entity': client_entity,
                    'caps': [
                        'mds', mds_cap_str,
                        'osd', osd_cap_str,
                        'mon', cap['caps'].get('mon', 'allow r')],
                })

### group operations

    def create_group(self, spec, mode=0o755, pool=None):
        path = spec.group_path
        self._mkdir_p(path, mode)
        if not pool:
            pool = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
        self.fs.setxattr(path, 'ceph.dir.layout.pool', pool.encode('utf-8'), 0)

    def remove_group(self, spec, force):
        path = spec.group_path
        try:
            self.fs.rmdir(path)
        except cephfs.ObjectNotFound:
            if not force:
                raise VolumeException(-errno.ENOENT, "Subvolume group '{0}' not found".format(spec.group_id))
        except cephfs.Error as e:
            raise VolumeException(e.args[0], e.args[1])

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
            raise VolumeException(e.args[0], e.args[1])
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
            raise VolumeException(e.args[0], e.args[1])

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

    ### context manager routines

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

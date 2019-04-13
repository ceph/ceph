
"""
Copyright (C) 2019 Red Hat, Inc.

LGPL2.1.  See file COPYING.
"""

import errno
import json
import logging
import os

from ceph_argparse import json_command

import cephfs
import rados

def to_bytes(param):
    '''
    Helper method that returns byte representation of the given parameter.
    '''
    if isinstance(param, str):
        return param.encode()
    else:
        return str(param).encode()

class RadosError(Exception):
    """
    Something went wrong talking to Ceph with librados
    """
    pass


RADOS_TIMEOUT = 10

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
    'Subvolume' concept implemented as a cephfs directory and
    client capabilities which restrict mount access to this
    directory.

    Additionally, subvolumes may be in a 'Group'.  Conveniently,
    subvolumes are a lot like manila shares, and groups are a lot
    like manila consistency groups.

    Refer to subvolumes with SubvolumePath, which specifies the
    subvolume and group IDs (both strings).  The group ID may
    be None.

    In general, functions in this class are allowed raise rados.Error
    or cephfs.Error exceptions in unexpected situations.
    """

    # Where shall we create our volumes?
    DEFAULT_SUBVOL_PREFIX = "/subvolumes"
    DEFAULT_NS_PREFIX = "fssubvolumens_"

    def __init__(self, subvolume_prefix=None, pool_ns_prefix=None, rados=None,
                 fs_name=None):
        self.fs = None
        self.fs_name = fs_name
        self.connected = False

        self.rados = rados

        self.subvolume_prefix = subvolume_prefix if subvolume_prefix else self.DEFAULT_SUBVOL_PREFIX
        self.pool_ns_prefix = pool_ns_prefix if pool_ns_prefix else self.DEFAULT_NS_PREFIX

    def get_mds_map(self):
        fs_map = self._rados_command("fs dump", {})
        return fs_map['filesystems'][0]['mdsmap']

    def _get_path(self, subvolume_path):
        """
        Determine the path within CephFS where this subvolume will live
        :return: absolute path (string)
        """
        return os.path.join(
            self.subvolume_prefix,
            subvolume_path.group_id if subvolume_path.group_id is not None else NO_GROUP_NAME,
            subvolume_path.subvolume_id)

    def _get_group_path(self, group_id):
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
        self.fs.mount(filesystem_name=self.fs_name)
        log.debug("Connection to cephfs complete")

    def get_mon_addrs(self):
        log.info("get_mon_addrs")
        result = []
        mon_map = self._rados_command("mon dump")
        for mon in mon_map['mons']:
            ip_port = mon['addr'].split("/")[0]
            result.append(ip_port)

        return result

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

    def create_subvolume(self, subvolume_path, size=None, namespace_isolated=True, mode=0o755):
        """
        Set up metadata, pools and auth for a subvolume.

        This function is idempotent.  It is safe to call this again
        for an already-created subvolume, even if it is in use.

        :param subvolume_path: SubvolumePath instance
        :param size: In bytes, or None for no size limit
        :param namespace_isolated: If true, use separate RADOS namespace for this volume
        :return:
        """
        path = self._get_path(subvolume_path)
        log.info("create_subvolume: {0}".format(path))

        self._mkdir_p(path, mode)

        if size is not None:
            self.fs.setxattr(path, 'ceph.quota.max_bytes', to_bytes(size), 0)

        # enforce security isolation, use separate namespace for this subvolume
        if namespace_isolated:
            namespace = "{0}{1}".format(self.pool_ns_prefix, subvolume_path.subvolume_id)
            log.info("create_subvolume: {0}, using rados namespace {1} to isolate data.".format(subvolume_path, namespace))
            self.fs.setxattr(path, 'ceph.dir.layout.pool_namespace',
                             to_bytes(namespace), 0)
        else:
            # If subvolume's namespace layout is not set, then the subvolume's pool
            # layout remains unset and will undesirably change with ancestor's
            # pool layout changes.
            pool_name = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
            self.fs.setxattr(path, 'ceph.dir.layout.pool',
                             to_bytes(pool_name), 0)

        return {
            'mount_path': path
        }

    def delete_subvolume(self, subvolume_path, data_isolated=False):
        """
        Make a subvolume inaccessible to guests.  This function is
        idempotent.  This is the fast part of tearing down a subvolume: you must
        also later call purge_subvolume, which is the slow part.

        :param subvolume_path: Same identifier used in create_subvolume
        :return:
        """

        path = self._get_path(subvolume_path)
        log.info("delete_subvolume: {0}".format(path))

        # Create the trash folder if it doesn't already exist
        trash = os.path.join(self.subvolume_prefix, "_deleting")
        self._mkdir_p(trash)

        # We'll move it to here
        trashed_subvolume = os.path.join(trash, subvolume_path.subvolume_id)

        # Move the subvolume's data to the trash folder
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            log.warning("Trying to delete subvolume '{0}' but it's already gone".format(
                path))
        else:
            self.fs.rename(path, trashed_subvolume)

    def purge_subvolume(self, subvolume_path, data_isolated=False):
        """
        Finish clearing up a subvolume that was previously passed to delete_subvolume.  This
        function is idempotent.
        """

        trash = os.path.join(self.subvolume_prefix, "_deleting")
        trashed_subvolume = os.path.join(trash, subvolume_path.subvolume_id)

        try:
            self.fs.stat(trashed_subvolume)
        except cephfs.ObjectNotFound:
            log.warning("Trying to purge subvolume '{0}' but it's already been purged".format(
                trashed_subvolume))
            return

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            dir_handle = self.fs.opendir(root_path)
            d = self.fs.readdir(dir_handle)
            while d:
                if d.d_name not in [".", ".."]:
                    # Do not use os.path.join because it is sensitive
                    # to string encoding, we just pass through dnames
                    # as byte arrays
                    d_full = "{0}/{1}".format(root_path, d.d_name)
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
            result = self.fs.getxattr(path, attr).decode()
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

    def authorize(self, subvolume_path, auth_id, readonly=False):
        """
        Get-or-create a Ceph auth identity for `auth_id` and grant them access
        to
        :param volume_path:
        :param auth_id:
        :param readonly:
        :return:
        """
        path = self._get_path(subvolume_path)
        log.debug("Authorizing Ceph id '{0}' for path '{1}'".format(
            auth_id, path
        ))

        # First I need to work out what the data pool is for this share:
        # read the layout
        pool_name = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")

        try:
            namespace = self.fs.getxattr(path, "ceph.dir.layout.pool_"
                                         "namespace").decode()
        except cephfs.NoData:
            namespace = None

        # Now construct auth capabilities that give the guest just enough
        # permissions to access the share
        client_entity = "client.{0}".format(auth_id)
        want_access_level = 'r' if readonly else 'rw'
        want_mds_cap = 'allow {0} path={1}'.format(want_access_level, path)
        if namespace:
            want_osd_cap = 'allow {0} pool={1} namespace={2}'.format(
                want_access_level, pool_name, namespace)
        else:
            want_osd_cap = 'allow {0} pool={1}'.format(want_access_level,
                                                       pool_name)

        try:
            existing = self._rados_command(
                'auth get',
                {
                    'entity': client_entity
                }
            )
            # FIXME: rados raising Error instead of ObjectNotFound in auth get failure
        except rados.Error:
            caps = self._rados_command(
                'auth get-or-create',
                {
                    'entity': client_entity,
                    'caps': [
                        'mds', want_mds_cap,
                        'osd', want_osd_cap,
                        'mon', 'allow r']
                })
        else:
            # entity exists, update it
            cap = existing[0]

            # Construct auth caps that if present might conflict with the desired
            # auth caps.
            unwanted_access_level = 'r' if want_access_level is 'rw' else 'rw'
            unwanted_mds_cap = 'allow {0} path={1}'.format(unwanted_access_level, path)
            if namespace:
                unwanted_osd_cap = 'allow {0} pool={1} namespace={2}'.format(
                    unwanted_access_level, pool_name, namespace)
            else:
                unwanted_osd_cap = 'allow {0} pool={1}'.format(
                    unwanted_access_level, pool_name)

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

            caps = self._rados_command(
                'auth caps',
                {
                    'entity': client_entity,
                    'caps': [
                        'mds', mds_cap_str,
                        'osd', osd_cap_str,
                        'mon', cap['caps'].get('mon', 'allow r')]
                })
            caps = self._rados_command(
                'auth get',
                {
                    'entity': client_entity
                }
            )

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
        assert len(caps) == 1
        assert caps[0]['entity'] == client_entity
        return caps[0]['key']

    def deauthorize(self, subvolume_path, auth_id):
        """
        The volume must still exist.
        """
        client_entity = "client.{0}".format(auth_id)
        path = self._get_path(subvolume_path)
        pool_name = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
        try:
            namespace = self.fs.getxattr(path, "ceph.dir.layout.pool_"
                                         "namespace").decode()
        except cephfs.NoData:
            namespace = None

        # The auth_id might have read-only or read-write mount access for the
        # volume path.
        access_levels = ('r', 'rw')
        want_mds_caps = ['allow {0} path={1}'.format(access_level, path)
                         for access_level in access_levels]
        if namespace:
            want_osd_caps = ['allow {0} pool={1} namespace={2}'.format(access_level, pool_name, namespace)
                             for access_level in access_levels]
        else:
            want_osd_caps = ['allow {0} pool={1}'.format(access_level, pool_name)
                             for access_level in access_levels]


        try:
            existing = self._rados_command(
                'auth get',
                {
                    'entity': client_entity
                }
            )

            def cap_remove(orig_mds_caps, orig_osd_caps, want_mds_caps, want_osd_caps):
                mds_cap_tokens = orig_mds_caps.split(",")
                osd_cap_tokens = orig_osd_caps.split(",")

                for want_mds_cap, want_osd_cap in zip(want_mds_caps, want_osd_caps):
                    if want_mds_cap in mds_cap_tokens:
                        mds_cap_tokens.remove(want_mds_cap)
                        osd_cap_tokens.remove(want_osd_cap)
                        break

                return ",".join(mds_cap_tokens), ",".join(osd_cap_tokens)

            cap = existing[0]
            orig_mds_caps = cap['caps'].get('mds', "")
            orig_osd_caps = cap['caps'].get('osd', "")
            mds_cap_str, osd_cap_str = cap_remove(orig_mds_caps, orig_osd_caps,
                                                  want_mds_caps, want_osd_caps)

            if not mds_cap_str:
                self._rados_command('auth del', {'entity': client_entity}, decode=False)
            else:
                self._rados_command(
                    'auth caps',
                    {
                        'entity': client_entity,
                        'caps': [
                            'mds', mds_cap_str,
                            'osd', osd_cap_str,
                            'mon', cap['caps'].get('mon', 'allow r')]
                    })

        # FIXME: rados raising Error instead of ObjectNotFound in auth get failure
        except rados.Error:
            # Already gone, great.
            return

    def _rados_command(self, prefix, args=None, decode=True):
        """
        Safer wrapper for ceph_argparse.json_command, which raises
        Error exception instead of relying on caller to check return
        codes.

        Error exception can result from:
        * Timeout
        * Actual legitimate errors
        * Malformed JSON output

        return: Decoded object from ceph, or None if empty string returned.
                If decode is False, return a string (the data returned by
                ceph command)
        """
        if args is None:
            args = {}

        argdict = args.copy()
        argdict['format'] = 'json'

        ret, outbuf, outs = json_command(self.rados,
                                         prefix=prefix,
                                         argdict=argdict,
                                         timeout=RADOS_TIMEOUT)
        if ret != 0:
            raise rados.Error(outs)
        else:
            if decode:
                if outbuf:
                    try:
                        return json.loads(outbuf.decode())
                    except (ValueError, TypeError):
                        raise RadosError("Invalid JSON output for command {0}".format(argdict))
                else:
                    return None
            else:
                return outbuf

    def get_used_bytes(self, subvolume_path):
        return int(self.fs.getxattr(self._get_path(subvolume_path), "ceph.dir."
                                    "rbytes").decode())

    def set_max_bytes(self, subvolume_path, max_bytes):
        self.fs.setxattr(self._get_path(subvolume_path), 'ceph.quota.max_bytes',
                         to_bytes(max_bytes if max_bytes else 0), 0)

    def _snapshot_path(self, dir_path, snapshot_name):
        return os.path.join(
            dir_path, self.rados.conf_get('client_snapdir'), snapshot_name
        )

    def _snapshot_create(self, dir_path, snapshot_name, mode=0o755):
        # TODO: raise intelligible exception for clusters where snaps are disabled
        self.fs.mkdir(self._snapshot_path(dir_path, snapshot_name), mode)

    def _snapshot_destroy(self, dir_path, snapshot_name):
        """
        Remove a snapshot, or do nothing if it already doesn't exist.
        """
        try:
            self.fs.rmdir(self._snapshot_path(dir_path, snapshot_name))
        except cephfs.ObjectNotFound:
            log.warn("Snapshot was already gone: {0}".format(snapshot_name))

    def create_snapshot_subvolume(self, subvolume_path, snapshot_name, mode=0o755):
        self._snapshot_create(self._get_path(subvolume_path), snapshot_name, mode)

    def destroy_snapshot_subvolume(self, subvolume_path, snapshot_name):
        self._snapshot_destroy(self._get_path(subvolume_path), snapshot_name)

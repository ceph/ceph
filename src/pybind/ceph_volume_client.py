"""
Copyright (C) 2015 Red Hat, Inc.

LGPL-2.1 or LGPL-3.0.  See file COPYING.
"""

from contextlib import contextmanager
import errno
import fcntl
import json
import logging
import os
import re
import struct
import sys
import threading
import time
import uuid

from ceph_argparse import json_command

import cephfs
import rados

def to_bytes(param):
    '''
    Helper method that returns byte representation of the given parameter.
    '''
    if isinstance(param, str):
        return param.encode('utf-8')
    elif param is None:
        return param
    else:
        return str(param).encode('utf-8')

class RadosError(Exception):
    """
    Something went wrong talking to Ceph with librados
    """
    pass


RADOS_TIMEOUT = 10

log = logging.getLogger(__name__)

# Reserved volume group name which we use in paths for volumes
# that are not assigned to a group (i.e. created with group=None)
NO_GROUP_NAME = "_nogroup"

# Filename extensions for meta files.
META_FILE_EXT = ".meta"

class VolumePath(object):
    """
    Identify a volume's path as group->volume
    The Volume ID is a unique identifier, but this is a much more
    helpful thing to pass around.
    """
    def __init__(self, group_id, volume_id):
        self.group_id = group_id
        self.volume_id = volume_id
        assert self.group_id != NO_GROUP_NAME
        assert self.volume_id != "" and self.volume_id is not None

    def __str__(self):
        return "{0}/{1}".format(self.group_id, self.volume_id)


class ClusterTimeout(Exception):
    """
    Exception indicating that we timed out trying to talk to the Ceph cluster,
    either to the mons, or to any individual daemon that the mons indicate ought
    to be up but isn't responding to us.
    """
    pass


class ClusterError(Exception):
    """
    Exception indicating that the cluster returned an error to a command that
    we thought should be successful based on our last knowledge of the cluster
    state.
    """
    def __init__(self, action, result_code, result_str):
        self._action = action
        self._result_code = result_code
        self._result_str = result_str

    def __str__(self):
        return "Error {0} (\"{1}\") while {2}".format(
            self._result_code, self._result_str, self._action)


class RankEvicter(threading.Thread):
    """
    Thread for evicting client(s) from a particular MDS daemon instance.

    This is more complex than simply sending a command, because we have to
    handle cases where MDS daemons might not be fully up yet, and/or might
    be transiently unresponsive to commands.
    """
    class GidGone(Exception):
        pass

    POLL_PERIOD = 5

    def __init__(self, volume_client, client_spec, rank, gid, mds_map, ready_timeout):
        """
        :param client_spec: list of strings, used as filter arguments to "session evict"
                            pass ["id=123"] to evict a single client with session id 123.
        """
        self.rank = rank
        self.gid = gid
        self._mds_map = mds_map
        self._client_spec = client_spec
        self._volume_client = volume_client
        self._ready_timeout = ready_timeout
        self._ready_waited = 0

        self.success = False
        self.exception = None

        super(RankEvicter, self).__init__()

    def _ready_to_evict(self):
        if self._mds_map['up'].get("mds_{0}".format(self.rank), None) != self.gid:
            log.info("Evicting {0} from {1}/{2}: rank no longer associated with gid, done.".format(
                self._client_spec, self.rank, self.gid
            ))
            raise RankEvicter.GidGone()

        info = self._mds_map['info']["gid_{0}".format(self.gid)]
        log.debug("_ready_to_evict: state={0}".format(info['state']))
        return info['state'] in ["up:active", "up:clientreplay"]

    def _wait_for_ready(self):
        """
        Wait for that MDS rank to reach an active or clientreplay state, and
        not be laggy.
        """
        while not self._ready_to_evict():
            if self._ready_waited > self._ready_timeout:
                raise ClusterTimeout()

            time.sleep(self.POLL_PERIOD)
            self._ready_waited += self.POLL_PERIOD

            self._mds_map = self._volume_client.get_mds_map()

    def _evict(self):
        """
        Run the eviction procedure.  Return true on success, false on errors.
        """

        # Wait til the MDS is believed by the mon to be available for commands
        try:
            self._wait_for_ready()
        except self.GidGone:
            return True

        # Then send it an evict
        ret = errno.ETIMEDOUT
        while ret == errno.ETIMEDOUT:
            log.debug("mds_command: {0}, {1}".format(
                "%s" % self.gid, ["session", "evict"] + self._client_spec
            ))
            ret, outb, outs = self._volume_client.fs.mds_command(
                "%s" % self.gid,
                [json.dumps({
                                "prefix": "session evict",
                                "filters": self._client_spec
                })], "")
            log.debug("mds_command: complete {0} {1}".format(ret, outs))

            # If we get a clean response, great, it's gone from that rank.
            if ret == 0:
                return True
            elif ret == errno.ETIMEDOUT:
                # Oh no, the MDS went laggy (that's how libcephfs knows to emit this error)
                self._mds_map = self._volume_client.get_mds_map()
                try:
                    self._wait_for_ready()
                except self.GidGone:
                    return True
            else:
                raise ClusterError("Sending evict to mds.{0}".format(self.gid), ret, outs)

    def run(self):
        try:
            self._evict()
        except Exception as e:
            self.success = False
            self.exception = e
        else:
            self.success = True


class EvictionError(Exception):
    pass


class CephFSVolumeClientError(Exception):
    """
    Something went wrong talking to Ceph using CephFSVolumeClient.
    """
    pass


CEPHFSVOLUMECLIENT_VERSION_HISTORY = """

    CephFSVolumeClient Version History:

    * 1 - Initial version
    * 2 - Added get_object, put_object, delete_object methods to CephFSVolumeClient
    * 3 - Allow volumes to be created without RADOS namespace isolation
    * 4 - Added get_object_and_version, put_object_versioned method to CephFSVolumeClient
"""


class CephFSVolumeClient(object):
    """
    Combine libcephfs and librados interfaces to implement a
    'Volume' concept implemented as a cephfs directory and
    client capabilities which restrict mount access to this
    directory.

    Additionally, volumes may be in a 'Group'.  Conveniently,
    volumes are a lot like manila shares, and groups are a lot
    like manila consistency groups.

    Refer to volumes with VolumePath, which specifies the
    volume and group IDs (both strings).  The group ID may
    be None.

    In general, functions in this class are allowed raise rados.Error
    or cephfs.Error exceptions in unexpected situations.
    """

    # Current version
    version = 4

    # Where shall we create our volumes?
    POOL_PREFIX = "fsvolume_"
    DEFAULT_VOL_PREFIX = "/volumes"
    DEFAULT_NS_PREFIX = "fsvolumens_"

    def __init__(self, auth_id=None, conf_path=None, cluster_name=None,
                 volume_prefix=None, pool_ns_prefix=None, rados=None,
                 fs_name=None):
        """
        Either set all three of ``auth_id``, ``conf_path`` and
        ``cluster_name`` (rados constructed on connect), or
        set ``rados`` (existing rados instance).
        """
        self.fs = None
        self.fs_name = fs_name
        self.connected = False

        self.conf_path = conf_path
        self.cluster_name = cluster_name
        self.auth_id = auth_id

        self.rados = rados
        if self.rados:
            # Using an externally owned rados, so we won't tear it down
            # on disconnect
            self.own_rados = False
        else:
            # self.rados will be constructed in connect
            self.own_rados = True

        self.volume_prefix = volume_prefix if volume_prefix else self.DEFAULT_VOL_PREFIX
        self.pool_ns_prefix = pool_ns_prefix if pool_ns_prefix else self.DEFAULT_NS_PREFIX
        # For flock'ing in cephfs, I want a unique ID to distinguish me
        # from any other manila-share services that are loading this module.
        # We could use pid, but that's unnecessary weak: generate a
        # UUID
        self._id = struct.unpack(">Q", uuid.uuid1().bytes[0:8])[0]

        # TODO: version the on-disk structures

    def recover(self):
        # Scan all auth keys to see if they're dirty: if they are, they have
        # state that might not have propagated to Ceph or to the related
        # volumes yet.

        # Important: we *always* acquire locks in the order auth->volume
        # That means a volume can never be dirty without the auth key
        # we're updating it with being dirty at the same time.

        # First list the auth IDs that have potentially dirty on-disk metadata
        log.debug("Recovering from partial auth updates (if any)...")

        try:
            dir_handle = self.fs.opendir(self.volume_prefix)
        except cephfs.ObjectNotFound:
            log.debug("Nothing to recover. No auth meta files.")
            return

        d = self.fs.readdir(dir_handle)
        auth_ids = []

        if not d:
            log.debug("Nothing to recover. No auth meta files.")

        while d:
            # Identify auth IDs from auth meta filenames. The auth meta files
            # are named as, "$<auth_id><meta filename extension>"
            regex = "^\$(.*){0}$".format(re.escape(META_FILE_EXT))
            match = re.search(regex, d.d_name.decode(encoding='utf-8'))
            if match:
                auth_ids.append(match.group(1))

            d = self.fs.readdir(dir_handle)

        self.fs.closedir(dir_handle)

        # Key points based on ordering:
        # * Anything added in VMeta is already added in AMeta
        # * Anything added in Ceph is already added in VMeta
        # * Anything removed in VMeta is already removed in Ceph
        # * Anything removed in AMeta is already removed in VMeta

        # Deauthorization: because I only update metadata AFTER the
        # update of the next level down, I have the same ordering of
        # -> things which exist in the AMeta should also exist
        #    in the VMeta, should also exist in Ceph, and the same
        #    recovery procedure that gets me consistent after crashes
        #    during authorization will also work during deauthorization

        # Now for each auth ID, check for dirty flag and apply updates
        # if dirty flag is found
        for auth_id in auth_ids:
            with self._auth_lock(auth_id):
                auth_meta = self._auth_metadata_get(auth_id)
                if not auth_meta or not auth_meta['volumes']:
                    # Clean up auth meta file
                    self.fs.unlink(self._auth_metadata_path(auth_id))
                    continue
                if not auth_meta['dirty']:
                    continue
                self._recover_auth_meta(auth_id, auth_meta)

        log.debug("Recovered from partial auth updates (if any).")

    def _recover_auth_meta(self, auth_id, auth_meta):
        """
        Call me after locking the auth meta file.
        """
        remove_volumes = []

        for volume, volume_data in auth_meta['volumes'].items():
            if not volume_data['dirty']:
                continue

            (group_id, volume_id) = volume.split('/')
            group_id = group_id if group_id is not 'None' else None
            volume_path = VolumePath(group_id, volume_id)
            access_level = volume_data['access_level']

            with self._volume_lock(volume_path):
                vol_meta = self._volume_metadata_get(volume_path)

                # No VMeta update indicates that there was no auth update
                # in Ceph either. So it's safe to remove corresponding
                # partial update in AMeta.
                if not vol_meta or auth_id not in vol_meta['auths']:
                    remove_volumes.append(volume)
                    continue

                want_auth = {
                    'access_level': access_level,
                    'dirty': False,
                }
                # VMeta update looks clean. Ceph auth update must have been
                # clean.
                if vol_meta['auths'][auth_id] == want_auth:
                    continue

                readonly = True if access_level is 'r' else False
                self._authorize_volume(volume_path, auth_id, readonly)

            # Recovered from partial auth updates for the auth ID's access
            # to a volume.
            auth_meta['volumes'][volume]['dirty'] = False
            self._auth_metadata_set(auth_id, auth_meta)

        for volume in remove_volumes:
            del auth_meta['volumes'][volume]

        if not auth_meta['volumes']:
            # Clean up auth meta file
            self.fs.unlink(self._auth_metadata_path(auth_id))
            return

        # Recovered from all partial auth updates for the auth ID.
        auth_meta['dirty'] = False
        self._auth_metadata_set(auth_id, auth_meta)

    def get_mds_map(self):
        fs_map = self._rados_command("fs dump", {})
        return fs_map['filesystems'][0]['mdsmap']

    def evict(self, auth_id, timeout=30, volume_path=None):
        """
        Evict all clients based on the authorization ID and optionally based on
        the volume path mounted.  Assumes that the authorization key has been
        revoked prior to calling this function.

        This operation can throw an exception if the mon cluster is unresponsive, or
        any individual MDS daemon is unresponsive for longer than the timeout passed in.
        """

        client_spec = ["auth_name={0}".format(auth_id), ]
        if volume_path:
            client_spec.append("client_metadata.root={0}".
                               format(self._get_path(volume_path)))

        log.info("evict clients with {0}".format(', '.join(client_spec)))

        mds_map = self.get_mds_map()
        up = {}
        for name, gid in mds_map['up'].items():
            # Quirk of the MDSMap JSON dump: keys in the up dict are like "mds_0"
            assert name.startswith("mds_")
            up[int(name[4:])] = gid

        # For all MDS ranks held by a daemon
        # Do the parallelism in python instead of using "tell mds.*", because
        # the latter doesn't give us per-mds output
        threads = []
        for rank, gid in up.items():
            thread = RankEvicter(self, client_spec, rank, gid, mds_map,
                                 timeout)
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()

        log.info("evict: joined all")

        for t in threads:
            if not t.success:
                msg = ("Failed to evict client with {0} from mds {1}/{2}: {3}".
                       format(', '.join(client_spec), t.rank, t.gid, t.exception)
                      )
                log.error(msg)
                raise EvictionError(msg)

    def _get_path(self, volume_path):
        """
        Determine the path within CephFS where this volume will live
        :return: absolute path (string)
        """
        return os.path.join(
            self.volume_prefix,
            volume_path.group_id if volume_path.group_id is not None else NO_GROUP_NAME,
            volume_path.volume_id)

    def _get_group_path(self, group_id):
        if group_id is None:
            raise ValueError("group_id may not be None")

        return os.path.join(
            self.volume_prefix,
            group_id
        )

    def _connect(self, premount_evict):
        log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.rados)
        log.debug("CephFS initializing...")
        self.fs.init()
        if premount_evict is not None:
            log.debug("Premount eviction of {0} starting".format(premount_evict))
            self.evict(premount_evict)
            log.debug("Premount eviction of {0} completes".format(premount_evict))
        log.debug("CephFS mounting...")
        self.fs.mount(filesystem_name=to_bytes(self.fs_name))
        log.debug("Connection to cephfs complete")

        # Recover from partial auth updates due to a previous
        # crash.
        self.recover()

    def connect(self, premount_evict = None):
        """

        :param premount_evict: Optional auth_id to evict before mounting the filesystem: callers
                               may want to use this to specify their own auth ID if they expect
                               to be a unique instance and don't want to wait for caps to time
                               out after failure of another instance of themselves.
        """
        if self.own_rados:
            log.debug("Configuring to RADOS with config {0}...".format(self.conf_path))
            self.rados = rados.Rados(
                name="client.{0}".format(self.auth_id),
                clustername=self.cluster_name,
                conffile=self.conf_path,
                conf={}
            )
            if self.rados.state != "connected":
                log.debug("Connecting to RADOS...")
                self.rados.connect()
                log.debug("Connection to RADOS complete")
        self._connect(premount_evict)

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

        if self.rados and self.own_rados:
            log.debug("Disconnecting rados...")
            self.rados.shutdown()
            self.rados = None
            log.debug("Disconnecting rados complete")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def _get_pool_id(self, osd_map, pool_name):
        # Maybe borrow the OSDMap wrapper class from calamari if more helpers
        # like this are needed.
        for pool in osd_map['pools']:
            if pool['pool_name'] == pool_name:
                return pool['pool']

        return None

    def _create_volume_pool(self, pool_name):
        """
        Idempotently create a pool for use as a CephFS data pool, with the given name

        :return The ID of the created pool
        """
        osd_map = self._rados_command('osd dump', {})

        existing_id = self._get_pool_id(osd_map, pool_name)
        if existing_id is not None:
            log.info("Pool {0} already exists".format(pool_name))
            return existing_id

        self._rados_command(
            'osd pool create',
            {
                'pool': pool_name,
            }
        )

        osd_map = self._rados_command('osd dump', {})
        pool_id = self._get_pool_id(osd_map, pool_name)

        if pool_id is None:
            # If the pool isn't there, that's either a ceph bug, or it's some outside influence
            # removing it right after we created it.
            log.error("OSD map doesn't contain expected pool '{0}':\n{1}".format(
                pool_name, json.dumps(osd_map, indent=2)
            ))
            raise RuntimeError("Pool '{0}' not present in map after creation".format(pool_name))
        else:
            return pool_id

    def create_group(self, group_id, mode=0o755):
        # Prevent craftily-named volume groups from colliding with the meta
        # files.
        if group_id.endswith(META_FILE_EXT):
            raise ValueError("group ID cannot end with '{0}'.".format(
                META_FILE_EXT))
        path = self._get_group_path(group_id)
        self._mkdir_p(path, mode)

    def destroy_group(self, group_id):
        path = self._get_group_path(group_id)
        try:
            self.fs.stat(self.volume_prefix)
        except cephfs.ObjectNotFound:
            pass
        else:
            self.fs.rmdir(path)

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

    def create_volume(self, volume_path, size=None, data_isolated=False, namespace_isolated=True,
                      mode=0o755):
        """
        Set up metadata, pools and auth for a volume.

        This function is idempotent.  It is safe to call this again
        for an already-created volume, even if it is in use.

        :param volume_path: VolumePath instance
        :param size: In bytes, or None for no size limit
        :param data_isolated: If true, create a separate OSD pool for this volume
        :param namespace_isolated: If true, use separate RADOS namespace for this volume
        :return:
        """
        path = self._get_path(volume_path)
        log.info("create_volume: {0}".format(path))

        self._mkdir_p(path, mode)

        if size is not None:
            self.fs.setxattr(path, 'ceph.quota.max_bytes', to_bytes(size), 0)

        # data_isolated means create a separate pool for this volume
        if data_isolated:
            pool_name = "{0}{1}".format(self.POOL_PREFIX, volume_path.volume_id)
            log.info("create_volume: {0}, create pool {1} as data_isolated =True.".format(volume_path, pool_name))
            pool_id = self._create_volume_pool(pool_name)
            mds_map = self.get_mds_map()
            if pool_id not in mds_map['data_pools']:
                self._rados_command("fs add_data_pool", {
                    'fs_name': mds_map['fs_name'],
                    'pool': pool_name
                })
            time.sleep(5) # time for MDSMap to be distributed
            self.fs.setxattr(path, 'ceph.dir.layout.pool', to_bytes(pool_name), 0)

        # enforce security isolation, use separate namespace for this volume
        if namespace_isolated:
            namespace = "{0}{1}".format(self.pool_ns_prefix, volume_path.volume_id)
            log.info("create_volume: {0}, using rados namespace {1} to isolate data.".format(volume_path, namespace))
            self.fs.setxattr(path, 'ceph.dir.layout.pool_namespace',
                             to_bytes(namespace), 0)
        else:
            # If volume's namespace layout is not set, then the volume's pool
            # layout remains unset and will undesirably change with ancestor's
            # pool layout changes.
            pool_name = self._get_ancestor_xattr(path, "ceph.dir.layout.pool")
            self.fs.setxattr(path, 'ceph.dir.layout.pool',
                             to_bytes(pool_name), 0)

        # Create a volume meta file, if it does not already exist, to store
        # data about auth ids having access to the volume
        fd = self.fs.open(self._volume_metadata_path(volume_path),
                          os.O_CREAT, 0o755)
        self.fs.close(fd)

        return {
            'mount_path': path
        }

    def delete_volume(self, volume_path, data_isolated=False):
        """
        Make a volume inaccessible to guests.  This function is
        idempotent.  This is the fast part of tearing down a volume: you must
        also later call purge_volume, which is the slow part.

        :param volume_path: Same identifier used in create_volume
        :return:
        """

        path = self._get_path(volume_path)
        log.info("delete_volume: {0}".format(path))

        # Create the trash folder if it doesn't already exist
        trash = os.path.join(self.volume_prefix, "_deleting")
        self._mkdir_p(trash)

        # We'll move it to here
        trashed_volume = os.path.join(trash, volume_path.volume_id)

        # Move the volume's data to the trash folder
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            log.warning("Trying to delete volume '{0}' but it's already gone".format(
                path))
        else:
            self.fs.rename(path, trashed_volume)

        # Delete the volume meta file, if it's not already deleted
        vol_meta_path = self._volume_metadata_path(volume_path)
        try:
            self.fs.unlink(vol_meta_path)
        except cephfs.ObjectNotFound:
            pass

    def purge_volume(self, volume_path, data_isolated=False):
        """
        Finish clearing up a volume that was previously passed to delete_volume.  This
        function is idempotent.
        """

        trash = os.path.join(self.volume_prefix, "_deleting")
        trashed_volume = os.path.join(trash, volume_path.volume_id)

        try:
            self.fs.stat(trashed_volume)
        except cephfs.ObjectNotFound:
            log.warning("Trying to purge volume '{0}' but it's already been purged".format(
                trashed_volume))
            return

        def rmtree(root_path):
            log.debug("rmtree {0}".format(root_path))
            dir_handle = self.fs.opendir(root_path)
            d = self.fs.readdir(dir_handle)
            while d:
                d_name = d.d_name.decode(encoding='utf-8')
                if d_name not in [".", ".."]:
                    # Do not use os.path.join because it is sensitive
                    # to string encoding, we just pass through dnames
                    # as byte arrays
                    d_full = u"{0}/{1}".format(root_path, d_name)
                    if d.is_dir():
                        rmtree(d_full)
                    else:
                        self.fs.unlink(d_full)

                d = self.fs.readdir(dir_handle)
            self.fs.closedir(dir_handle)

            self.fs.rmdir(root_path)

        rmtree(trashed_volume)

        if data_isolated:
            pool_name = "{0}{1}".format(self.POOL_PREFIX, volume_path.volume_id)
            osd_map = self._rados_command("osd dump", {})
            pool_id = self._get_pool_id(osd_map, pool_name)
            mds_map = self.get_mds_map()
            if pool_id in mds_map['data_pools']:
                self._rados_command("fs rm_data_pool", {
                    'fs_name': mds_map['fs_name'],
                    'pool': pool_name
                })
            self._rados_command("osd pool delete",
                                {
                                    "pool": pool_name,
                                    "pool2": pool_name,
                                    "yes_i_really_really_mean_it": True
                                })

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

    def _check_compat_version(self, compat_version):
        if self.version < compat_version:
            msg = ("The current version of CephFSVolumeClient, version {0} "
                   "does not support the required feature. Need version {1} "
                   "or greater".format(self.version, compat_version)
                  )
            log.error(msg)
            raise CephFSVolumeClientError(msg)

    def _metadata_get(self, path):
        """
        Return a deserialized JSON object, or None
        """
        fd = self.fs.open(path, "r")
        # TODO iterate instead of assuming file < 4MB
        read_bytes = self.fs.read(fd, 0, 4096 * 1024)
        self.fs.close(fd)
        if read_bytes:
            return json.loads(read_bytes.decode())
        else:
            return None

    def _metadata_set(self, path, data):
        serialized = json.dumps(data)
        fd = self.fs.open(path, "w")
        try:
            self.fs.write(fd, to_bytes(serialized), 0)
            self.fs.fsync(fd, 0)
        finally:
            self.fs.close(fd)

    def _lock(self, path):
        @contextmanager
        def fn():
            while(1):
                fd = self.fs.open(path, os.O_CREAT, 0o755)
                self.fs.flock(fd, fcntl.LOCK_EX, self._id)

                # The locked file will be cleaned up sometime. It could be
                # unlinked e.g., by an another manila share instance, before
                # lock was applied on it. Perform checks to ensure that this
                # does not happen.
                try:
                    statbuf = self.fs.stat(path)
                except cephfs.ObjectNotFound:
                    self.fs.close(fd)
                    continue

                fstatbuf = self.fs.fstat(fd)
                if statbuf.st_ino == fstatbuf.st_ino:
                    break

            try:
                yield
            finally:
                self.fs.flock(fd, fcntl.LOCK_UN, self._id)
                self.fs.close(fd)

        return fn()

    def _auth_metadata_path(self, auth_id):
        return os.path.join(self.volume_prefix, "${0}{1}".format(
            auth_id, META_FILE_EXT))

    def _auth_lock(self, auth_id):
        return self._lock(self._auth_metadata_path(auth_id))

    def _auth_metadata_get(self, auth_id):
        """
        Call me with the metadata locked!

        Check whether a auth metadata structure can be decoded by the current
        version of CephFSVolumeClient.

        Return auth metadata that the current version of CephFSVolumeClient
        can decode.
        """
        auth_metadata = self._metadata_get(self._auth_metadata_path(auth_id))

        if auth_metadata:
            self._check_compat_version(auth_metadata['compat_version'])

        return auth_metadata

    def _auth_metadata_set(self, auth_id, data):
        """
        Call me with the metadata locked!

        Fsync the auth metadata.

        Add two version attributes to the auth metadata,
        'compat_version', the minimum CephFSVolumeClient version that can
        decode the metadata, and 'version', the CephFSVolumeClient version
        that encoded the metadata.
        """
        data['compat_version'] = 1
        data['version'] = self.version
        return self._metadata_set(self._auth_metadata_path(auth_id), data)

    def _volume_metadata_path(self, volume_path):
        return os.path.join(self.volume_prefix, "_{0}:{1}{2}".format(
            volume_path.group_id if volume_path.group_id else "",
            volume_path.volume_id,
            META_FILE_EXT
        ))

    def _volume_lock(self, volume_path):
        """
        Return a ContextManager which locks the authorization metadata for
        a particular volume, and persists a flag to the metadata indicating
        that it is currently locked, so that we can detect dirty situations
        during recovery.

        This lock isn't just to make access to the metadata safe: it's also
        designed to be used over the two-step process of checking the
        metadata and then responding to an authorization request, to
        ensure that at the point we respond the metadata hasn't changed
        in the background.  It's key to how we avoid security holes
        resulting from races during that problem ,
        """
        return self._lock(self._volume_metadata_path(volume_path))

    def _volume_metadata_get(self, volume_path):
        """
        Call me with the metadata locked!

        Check whether a volume metadata structure can be decoded by the current
        version of CephFSVolumeClient.

        Return a volume_metadata structure that the current version of
        CephFSVolumeClient can decode.
        """
        volume_metadata = self._metadata_get(self._volume_metadata_path(volume_path))

        if volume_metadata:
            self._check_compat_version(volume_metadata['compat_version'])

        return volume_metadata

    def _volume_metadata_set(self, volume_path, data):
        """
        Call me with the metadata locked!

        Add two version attributes to the volume metadata,
        'compat_version', the minimum CephFSVolumeClient version that can
        decode the metadata and 'version', the CephFSVolumeClient version
        that encoded the metadata.
        """
        data['compat_version'] = 1
        data['version'] = self.version
        return self._metadata_set(self._volume_metadata_path(volume_path), data)

    def authorize(self, volume_path, auth_id, readonly=False, tenant_id=None):
        """
        Get-or-create a Ceph auth identity for `auth_id` and grant them access
        to
        :param volume_path:
        :param auth_id:
        :param readonly:
        :param tenant_id: Optionally provide a stringizable object to
                          restrict any created cephx IDs to other callers
                          passing the same tenant ID.
        :return:
        """

        with self._auth_lock(auth_id):
            # Existing meta, or None, to be updated
            auth_meta = self._auth_metadata_get(auth_id)

            # volume data to be inserted
            volume_path_str = str(volume_path)
            volume = {
                volume_path_str : {
                    # The access level at which the auth_id is authorized to
                    # access the volume.
                    'access_level': 'r' if readonly else 'rw',
                    'dirty': True,
                }
            }
            if auth_meta is None:
                sys.stderr.write("Creating meta for ID {0} with tenant {1}\n".format(
                    auth_id, tenant_id
                ))
                log.debug("Authorize: no existing meta")
                auth_meta = {
                    'dirty': True,
                    'tenant_id': tenant_id.__str__() if tenant_id else None,
                    'volumes': volume
                }

                # Note: this is *not* guaranteeing that the key doesn't already
                # exist in Ceph: we are allowing VolumeClient tenants to
                # 'claim' existing Ceph keys.  In order to prevent VolumeClient
                # tenants from reading e.g. client.admin keys, you need to
                # have configured your VolumeClient user (e.g. Manila) to
                # have mon auth caps that prevent it from accessing those keys
                # (e.g. limit it to only access keys with a manila.* prefix)
            else:
                # Disallow tenants to share auth IDs
                if auth_meta['tenant_id'].__str__() != tenant_id.__str__():
                    msg = "auth ID: {0} is already in use".format(auth_id)
                    log.error(msg)
                    raise CephFSVolumeClientError(msg)

                if auth_meta['dirty']:
                    self._recover_auth_meta(auth_id, auth_meta)

                log.debug("Authorize: existing tenant {tenant}".format(
                    tenant=auth_meta['tenant_id']
                ))
                auth_meta['dirty'] = True
                auth_meta['volumes'].update(volume)

            self._auth_metadata_set(auth_id, auth_meta)

            with self._volume_lock(volume_path):
                key = self._authorize_volume(volume_path, auth_id, readonly)

            auth_meta['dirty'] = False
            auth_meta['volumes'][volume_path_str]['dirty'] = False
            self._auth_metadata_set(auth_id, auth_meta)

            if tenant_id:
                return {
                    'auth_key': key
                }
            else:
                # Caller wasn't multi-tenant aware: be safe and don't give
                # them a key
                return {
                    'auth_key': None
                }

    def _authorize_volume(self, volume_path, auth_id, readonly):
        vol_meta = self._volume_metadata_get(volume_path)

        access_level = 'r' if readonly else 'rw'
        auth = {
            auth_id: {
                'access_level': access_level,
                'dirty': True,
            }
        }

        if vol_meta is None:
            vol_meta = {
                'auths': auth
            }
        else:
            vol_meta['auths'].update(auth)
            self._volume_metadata_set(volume_path, vol_meta)

        key = self._authorize_ceph(volume_path, auth_id, readonly)

        vol_meta['auths'][auth_id]['dirty'] = False
        self._volume_metadata_set(volume_path, vol_meta)

        return key

    def _authorize_ceph(self, volume_path, auth_id, readonly):
        path = self._get_path(volume_path)
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

    def deauthorize(self, volume_path, auth_id):
        with self._auth_lock(auth_id):
            # Existing meta, or None, to be updated
            auth_meta = self._auth_metadata_get(auth_id)

            volume_path_str = str(volume_path)
            if (auth_meta is None) or (not auth_meta['volumes']):
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for volume ID '{volume}'".format(
                    auth_id=auth_id, volume=volume_path.volume_id
                ))
                # Clean up the auth meta file of an auth ID
                self.fs.unlink(self._auth_metadata_path(auth_id))
                return

            if volume_path_str not in auth_meta['volumes']:
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for volume ID '{volume}'".format(
                    auth_id=auth_id, volume=volume_path.volume_id
                ))
                return

            if auth_meta['dirty']:
                self._recover_auth_meta(auth_id, auth_meta)

            auth_meta['dirty'] = True
            auth_meta['volumes'][volume_path_str]['dirty'] = True
            self._auth_metadata_set(auth_id, auth_meta)

            self._deauthorize_volume(volume_path, auth_id)

            # Filter out the volume we're deauthorizing
            del auth_meta['volumes'][volume_path_str]

            # Clean up auth meta file
            if not auth_meta['volumes']:
                self.fs.unlink(self._auth_metadata_path(auth_id))
                return

            auth_meta['dirty'] = False
            self._auth_metadata_set(auth_id, auth_meta)

    def _deauthorize_volume(self, volume_path, auth_id):
        with self._volume_lock(volume_path):
            vol_meta = self._volume_metadata_get(volume_path)

            if (vol_meta is None) or (auth_id not in vol_meta['auths']):
                log.warning("deauthorized called for already-removed auth"
                         "ID '{auth_id}' for volume ID '{volume}'".format(
                    auth_id=auth_id, volume=volume_path.volume_id
                ))
                return

            vol_meta['auths'][auth_id]['dirty'] = True
            self._volume_metadata_set(volume_path, vol_meta)

            self._deauthorize(volume_path, auth_id)

            # Remove the auth_id from the metadata *after* removing it
            # from ceph, so that if we crashed here, we would actually
            # recreate the auth ID during recovery (i.e. end up with
            # a consistent state).

            # Filter out the auth we're removing
            del vol_meta['auths'][auth_id]
            self._volume_metadata_set(volume_path, vol_meta)

    def _deauthorize(self, volume_path, auth_id):
        """
        The volume must still exist.
        """
        client_entity = "client.{0}".format(auth_id)
        path = self._get_path(volume_path)
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

    def get_authorized_ids(self, volume_path):
        """
        Expose a list of auth IDs that have access to a volume.

        return: a list of (auth_id, access_level) tuples, where
                the access_level can be 'r' , or 'rw'.
                None if no auth ID is given access to the volume.
        """
        with self._volume_lock(volume_path):
            meta = self._volume_metadata_get(volume_path)
            auths = []
            if not meta or not meta['auths']:
                return None

            for auth, auth_data in meta['auths'].items():
                # Skip partial auth updates.
                if not auth_data['dirty']:
                    auths.append((auth, auth_data['access_level']))

            return auths

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

    def get_used_bytes(self, volume_path):
        return int(self.fs.getxattr(self._get_path(volume_path), "ceph.dir."
                                    "rbytes").decode())

    def set_max_bytes(self, volume_path, max_bytes):
        self.fs.setxattr(self._get_path(volume_path), 'ceph.quota.max_bytes',
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
            log.warning("Snapshot was already gone: {0}".format(snapshot_name))

    def create_snapshot_volume(self, volume_path, snapshot_name, mode=0o755):
        self._snapshot_create(self._get_path(volume_path), snapshot_name, mode)

    def destroy_snapshot_volume(self, volume_path, snapshot_name):
        self._snapshot_destroy(self._get_path(volume_path), snapshot_name)

    def create_snapshot_group(self, group_id, snapshot_name, mode=0o755):
        if group_id is None:
            raise RuntimeError("Group ID may not be None")

        return self._snapshot_create(self._get_group_path(group_id), snapshot_name,
                                     mode)

    def destroy_snapshot_group(self, group_id, snapshot_name):
        if group_id is None:
            raise RuntimeError("Group ID may not be None")
        if snapshot_name is None:
            raise RuntimeError("Snapshot name may not be None")

        return self._snapshot_destroy(self._get_group_path(group_id), snapshot_name)

    def _cp_r(self, src, dst):
        # TODO
        raise NotImplementedError()

    def clone_volume_to_existing(self, dest_volume_path, src_volume_path, src_snapshot_name):
        dest_fs_path = self._get_path(dest_volume_path)
        src_snapshot_path = self._snapshot_path(self._get_path(src_volume_path), src_snapshot_name)

        self._cp_r(src_snapshot_path, dest_fs_path)

    def put_object(self, pool_name, object_name, data):
        """
        Synchronously write data to an object.

        :param pool_name: name of the pool
        :type pool_name: str
        :param object_name: name of the object
        :type object_name: str
        :param data: data to write
        :type data: bytes
        """
        return self.put_object_versioned(pool_name, object_name, data)

    def put_object_versioned(self, pool_name, object_name, data, version=None):
        """
        Synchronously write data to an object only if version of the object
        version matches the expected version.

        :param pool_name: name of the pool
        :type pool_name: str
        :param object_name: name of the object
        :type object_name: str
        :param data: data to write
        :type data: bytes
        :param version: expected version of the object to write
        :type version: int
        """
        ioctx = self.rados.open_ioctx(pool_name)

        max_size = int(self.rados.conf_get('osd_max_write_size')) * 1024 * 1024
        if len(data) > max_size:
            msg = ("Data to be written to object '{0}' exceeds "
                   "{1} bytes".format(object_name, max_size))
            log.error(msg)
            raise CephFSVolumeClientError(msg)

        try:
            with rados.WriteOpCtx() as wop:
                if version is not None:
                    wop.assert_version(version)
                wop.write_full(data)
                ioctx.operate_write_op(wop, object_name)
        except rados.OSError as e:
            log.error(e)
            raise e
        finally:
            ioctx.close()

    def get_object(self, pool_name, object_name):
        """
        Synchronously read data from object.

        :param pool_name: name of the pool
        :type pool_name: str
        :param object_name: name of the object
        :type object_name: str

        :returns: bytes - data read from object
        """
        return self.get_object_and_version(pool_name, object_name)[0]

    def get_object_and_version(self, pool_name, object_name):
        """
        Synchronously read data from object and get its version.

        :param pool_name: name of the pool
        :type pool_name: str
        :param object_name: name of the object
        :type object_name: str

        :returns: tuple of object data and version
        """
        ioctx = self.rados.open_ioctx(pool_name)
        max_size = int(self.rados.conf_get('osd_max_write_size')) * 1024 * 1024
        try:
            bytes_read = ioctx.read(object_name, max_size)
            if ((len(bytes_read) == max_size) and
                    (ioctx.read(object_name, 1, offset=max_size))):
                log.warning("Size of object {0} exceeds '{1}' bytes "
                            "read".format(object_name, max_size))
            obj_version = ioctx.get_last_version()
        finally:
            ioctx.close()
        return (bytes_read, obj_version)

    def delete_object(self, pool_name, object_name):
        ioctx = self.rados.open_ioctx(pool_name)
        try:
            ioctx.remove_object(object_name)
        except rados.ObjectNotFound:
            log.warning("Object '{0}' was already removed".format(object_name))
        finally:
            ioctx.close()

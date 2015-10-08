"""
Copyright (C) 2015 Red Hat, Inc.

LGPL2.  See file COPYING.
"""

import json
import logging
import os
import threading
import errno
import time

import rados
import cephfs
from ceph_argparse import json_command

# Generate missing lib errors at load time, rather than the
# first time someone tries to use the FS
try:
    cephfs.load_libcephfs()
except EnvironmentError as e:
    raise ImportError(e.__str__())



class RadosError(Exception):
    """
    Something went wrong talking to Ceph with librados
    """
    pass


RADOS_TIMEOUT = 10
SNAP_DIR = ".snap"

log = logging.getLogger(__name__)


# Reserved volume group name which we use in paths for volumes
# that
NO_GROUP_NAME = "_nogroup"


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
                self._client_spec,
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

            self._mds_map = self._volume_client._rados_command("mds dump", {})

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
                self._mds_map = self._volume_client._rados_command("mds dump", {})
                try:
                    self._wait_for_ready()
                except self.GidGone:
                    return True
            else:
                raise ClusterError("Sending evict to mds.{0}".format(self.gid), ret, outs)

    def run(self):
        try:
            self._evict()
        except Exception, e:
            self.success = False
            self.exception = e
        else:
            self.success = True


class EvictionError(Exception):
    pass


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

    # Where shall we create our volumes?
    VOLUME_PREFIX = "/volumes"
    POOL_PREFIX = "fsvolume_"

    def __init__(self, auth_id, conf_path, cluster_name):
        self.fs = None
        self.rados = None
        self.connected = False
        self.conf_path = conf_path
        self.cluster_name = cluster_name
        self.auth_id = auth_id

    def evict(self, auth_id, timeout=30):
        """
        Evict all clients using this authorization ID. Assumes that the
        authorisation key has been revoked prior to calling this function.

        This operation can throw an exception if the mon cluster is unresponsive, or
        any individual MDS daemon is unresponsive for longer than the timeout passed in.
        """

        log.info("evict: {0}".format(auth_id))

        mds_map = self._rados_command("mds dump", {})

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
            thread = RankEvicter(self, ["auth_name={0}".format(auth_id)], rank, gid, mds_map, timeout)
            thread.start()
            threads.append(thread)

        for t in threads:
            t.join()
        #
        # threads = []
        # for rank, gid in up.items():
        #     thread = RankEvicter(self, ["auth_name={0}".format(auth_id)], rank, gid, mds_map, timeout)
        #     thread.run()
        #     threads.append(thread)

        log.info("evict: joined all")

        for t in threads:
            if not t.success:
                msg = "Failed to evict client {0} from mds {1}/{2}: {3}".format(
                    auth_id, t.rank, t.gid, t.exception
                )
                log.error(msg)
                raise EvictionError(msg)

    def _get_path(self, volume_path):
        """
        Determine the path within CephFS where this volume will live
        :return: absolute path (string)
        """
        return os.path.join(
            self.VOLUME_PREFIX,
            volume_path.group_id if volume_path.group_id is not None else NO_GROUP_NAME,
            volume_path.volume_id)

    def _get_group_path(self, group_id):
        return os.path.join(
            self.VOLUME_PREFIX,
            group_id
        )

    def connect(self, premount_evict = None):
        """

        :param premount_evict: Optional auth_id to evict before mounting the filesystem: callers
                               may want to use this to specify their own auth ID if they expect
                               to be a unique instance and don't want to wait for caps to time
                               out after failure of another instance of themselves.
        """
        log.debug("Connecting to RADOS with config {0}...".format(self.conf_path))
        self.rados = rados.Rados(
            name="client.{0}".format(self.auth_id),
            clustername=self.cluster_name,
            conffile=self.conf_path,
            conf={}
        )
        self.rados.connect()

        log.debug("Connection to RADOS complete")

        log.debug("Connecting to cephfs...")
        self.fs = cephfs.LibCephFS(rados_inst=self.rados)
        log.debug("CephFS initializing...")
        self.fs.init()
        if premount_evict is not None:
            log.debug("Premount eviction of {0} starting".format(premount_evict))
            self.evict(premount_evict)
            log.debug("Premount eviction of {0} completes".format(premount_evict))
        log.debug("CephFS mounting...")
        self.fs.mount()
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

        if self.rados:
            log.debug("Disconnecting rados...")
            self.rados.shutdown()
            self.rados = None
            log.debug("Disconnecting rados complete")

    def __del__(self):
        self.disconnect()

    def _get_pool_id(self, osd_map, pool_name):
        # Maybe borrow the OSDMap wrapper class from calamari if more helpers like this aren needed.
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

        osd_count = len(osd_map['osds'])

        # We can't query the actual cluster config remotely, but since this is
        # just a heuristic we'll assume that the ceph.conf we have locally reflects
        # that in use in the rest of the cluster.
        pg_warn_max_per_osd = int(self.rados.conf_get('mon_pg_warn_max_per_osd'))

        other_pgs = 0
        for pool in osd_map['pools']:
            if not pool['pool_name'].startswith(self.POOL_PREFIX):
                other_pgs += pool['pg_num']

        # A basic heuristic for picking pg_num: work out the max number of
        # PGs we can have without tripping a warning, then subtract the number
        # of PGs already created by non-manila pools, then divide by ten.  That'll
        # give you a reasonable result on a system where you have "a few" manila
        # shares.
        pg_num = ((pg_warn_max_per_osd * osd_count) - other_pgs) / 10
        # TODO Alternatively, respect an override set by the user.

        self._rados_command(
            'osd pool create',
            {
                'pool': pool_name,
                'pg_num': pg_num
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

    def create_group(self, group_id):
        path = self._get_group_path(group_id)
        self._mkdir_p(path)

    def destroy_group(self, group_id):
        path = self._get_group_path(group_id)
        try:
            self.fs.stat(self.VOLUME_PREFIX)
        except cephfs.ObjectNotFound:
            pass
        else:
            self.fs.rmdir(path)

    def _mkdir_p(self, path):
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            pass
        else:
            return

        parts = os.path.split(path)
        for i in range(1, len(parts) + 1):
            subpath = os.path.join(*parts[0:i])
            try:
                self.fs.stat(subpath)
            except cephfs.ObjectNotFound:
                self.fs.mkdir(subpath, 0755)

    def create_volume(self, volume_path, size=None, data_isolated=False):
        """
        Set up metadata, pools and auth for a volume.

        This function is idempotent.  It is safe to call this again
        for an already-created volume, even if it is in use.

        :param volume_path: VolumePath instance
        :param size: In bytes, or None for no size limit
        :param data_isolated: If true, create a separate OSD pool for this volume
        :return:
        """
        log.info("create_volume: {0}".format(volume_path))
        path = self._get_path(volume_path)

        # # Fast pre-check: if the auth key (last thing created) exists, then
        # # the rest of the volume already exists too.
        # try:
        #     auth = self._rados_command("auth get", {"entity": client_entity})[0]
        # except rados.Error:
        #     pass
        # else:
        #     log.info("create_volume: {0} already exists, returning")
        #     return {
        #         'volume_key': auth['key'],
        #         'mount_path': path
        #     }

        self._mkdir_p(path)

        if size is not None:
            self.fs.setxattr(path, 'ceph.quota.max_bytes', size.__str__(), 0)

        if data_isolated:
            pool_name = "{0}{1}".format(self.POOL_PREFIX, volume_path.volume_id)
            pool_id = self._create_volume_pool(pool_name)
            mds_map = self._rados_command("mds dump", {})
            if pool_id not in mds_map['data_pools']:
                self._rados_command("mds add_data_pool", {
                    'pool': pool_name
                })
            self.fs.setxattr(path, 'ceph.dir.layout.pool', pool_name, 0)

        return {
            'mount_path': path
        }

    def delete_volume(self, volume_path, data_isolated=False):
        """
        Remove all trace of a volume from the Ceph cluster.  This function is
        idempotent.

        :param volume_path: Same identifier used in create_volume
        :return:
        """

        log.info("delete_volume: {0}".format(volume_path))

        # Create the trash folder if it doesn't already exist
        trash = os.path.join(self.VOLUME_PREFIX, "_deleting")
        self._mkdir_p(trash)

        # We'll move it to here
        trashed_volume = os.path.join(trash, volume_path.volume_id)

        # Move the volume's data to the trash folder
        path = self._get_path(volume_path)
        try:
            self.fs.stat(path)
        except cephfs.ObjectNotFound:
            log.warning("Trying to delete volume '{0}' but it's already gone".format(
                path))
        else:
            self.fs.rename(path, trashed_volume)

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
                if d.d_name not in [".", ".."]:
                    d_full = os.path.join(root_path, d.d_name)
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
            mds_map = self._rados_command("mds dump", {})
            if pool_id in mds_map['data_pools']:
                self._rados_command("mds remove_data_pool", {
                    'pool': pool_name
                })
            self._rados_command("osd pool delete",
                                {
                                    "pool": pool_name,
                                    "pool2": pool_name,
                                    "sure": "--yes-i-really-really-mean-it"
                                })

    def authorize(self, volume_path, auth_id):
        """
        Get-or-create a Ceph auth identity for `auth_id` and grant them access
        to
        :param volume_path:
        :param auth_id:
        :return:
        """

        path = self._get_path(volume_path)
        client_entity = "client.{0}".format(auth_id)
        caps = self._rados_command(
            'auth get-or-create',
            {
                'entity': client_entity,
                'caps': [
                    'mds', 'allow rw path={0}'.format(path),
                    'osd', 'allow rw',
                    'mon', 'allow r']
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

        assert len(caps) == 1
        assert caps[0]['entity'] == client_entity
        key = caps[0]['key']

        return key

    def deauthorize(self, volume_path, auth_id):
        client_entity = "client.{0}".format(auth_id)

        # Remove the auth key for this volume
        self._rados_command('auth del', {'entity': client_entity}, decode=False)

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
                        return json.loads(outbuf)
                    except (ValueError, TypeError):
                        raise RadosError("Invalid JSON output for command {0}".format(argdict))
                else:
                    return None
            else:
                return outbuf

    def get_used_bytes(self, volume_path):
        return int(self.fs.getxattr(self._get_path(volume_path), "ceph.dir.rbytes"))

    def set_max_bytes(self, volume_path, max_bytes):
        self.fs.setxattr(self._get_path(volume_path), 'ceph.quota.max_bytes',
                         max_bytes.__str__() if max_bytes is not None else "0",
                         0)

    def _snapshot_path(self, dir_path, snapshot_name):
        return os.path.join(
            dir_path, SNAP_DIR, snapshot_name
        )

    def _snapshot_create(self, dir_path, snapshot_name):
        # TODO: raise intelligible exception for clusters where snaps are disabled
        self.fs.mkdir(self._snapshot_path(dir_path, snapshot_name), 0755)

    def _snapshot_destroy(self, dir_path, snapshot_name):
        """
        Remove a snapshot, or do nothing if it already doesn't exist.
        """
        try:
            self.fs.rmdir(self._snapshot_path(dir_path, snapshot_name))
        except cephfs.ObjectNotFound:
            log.warn("Snapshot was already gone: {0}".format(snapshot_name))

    def create_snapshot_volume(self, volume_path, snapshot_name):
        self._snapshot_create(self._get_path(volume_path), snapshot_name)

    def destroy_snapshot_volume(self, volume_path, snapshot_name):
        self._snapshot_destroy(self._get_path(volume_path), snapshot_name)

    def create_snapshot_group(self, group_id, snapshot_name):
        if group_id is None:
            raise RuntimeError("Group ID may not be None")

        return self._snapshot_create(self._get_group_path(group_id), snapshot_name)

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

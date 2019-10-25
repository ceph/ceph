import json
import time
import errno
import logging
from threading import Lock
try:
    # py2
    from threading import _Timer as Timer
except ImportError:
    #py3
    from threading import Timer

import cephfs
import orchestrator

from .subvolspec import SubvolumeSpec
from .subvolume import SubVolume
from .exception import VolumeException
from .purge_queue import ThreadPoolPurgeQueueMixin

log = logging.getLogger(__name__)

class ConnectionPool(object):
    class Connection(object):
        def __init__(self, mgr, fs_name):
            self.fs = None
            self.mgr = mgr
            self.fs_name = fs_name
            self.ops_in_progress = 0
            self.last_used = time.time()
            self.fs_id = self.get_fs_id()

        def get_fs_id(self):
            fs_map = self.mgr.get('fs_map')
            for fs in fs_map['filesystems']:
                if fs['mdsmap']['fs_name'] == self.fs_name:
                    return fs['id']
            raise VolumeException(
                -errno.ENOENT, "Volume '{0}' not found".format(self.fs_name))

        def get_fs_handle(self):
            self.last_used = time.time()
            self.ops_in_progress += 1
            return self.fs

        def put_fs_handle(self):
            assert self.ops_in_progress > 0
            self.ops_in_progress -= 1

        def del_fs_handle(self):
            if self.is_connection_valid():
                self.disconnect()
            else:
                self.abort()

        def is_connection_valid(self):
            fs_id = None
            try:
                fs_id = self.get_fs_id()
            except:
                # the filesystem does not exist now -- connection is not valid.
                pass
            return self.fs_id == fs_id

        def is_connection_idle(self, timeout):
            return (self.ops_in_progress == 0 and ((time.time() - self.last_used) >= timeout))

        def connect(self):
            assert self.ops_in_progress == 0
            log.debug("Connecting to cephfs '{0}'".format(self.fs_name))
            self.fs = cephfs.LibCephFS(rados_inst=self.mgr.rados)
            log.debug("Setting user ID and group ID of CephFS mount as root...")
            self.fs.conf_set("client_mount_uid", "0")
            self.fs.conf_set("client_mount_gid", "0")
            log.debug("CephFS initializing...")
            self.fs.init()
            log.debug("CephFS mounting...")
            self.fs.mount(filesystem_name=self.fs_name.encode('utf-8'))
            log.debug("Connection to cephfs '{0}' complete".format(self.fs_name))

        def disconnect(self):
            assert self.ops_in_progress == 0
            log.info("disconnecting from cephfs '{0}'".format(self.fs_name))
            self.fs.shutdown()
            self.fs = None

        def abort(self):
            assert self.ops_in_progress == 0
            log.info("aborting connection from cephfs '{0}'".format(self.fs_name))
            self.fs.abort_conn()
            self.fs = None

    class RTimer(Timer):
        """
        recurring timer variant of Timer
        """
        def run(self):
            while not self.finished.is_set():
                self.finished.wait(self.interval)
                self.function(*self.args, **self.kwargs)
            self.finished.set()

    # TODO: make this configurable
    TIMER_TASK_RUN_INTERVAL = 30.0  # seconds
    CONNECTION_IDLE_INTERVAL = 60.0 # seconds

    def __init__(self, mgr):
        self.mgr = mgr
        self.connections = {}
        self.lock = Lock()
        self.timer_task = ConnectionPool.RTimer(ConnectionPool.TIMER_TASK_RUN_INTERVAL,
                                                self.cleanup_connections)
        self.timer_task.start()

    def cleanup_connections(self):
        with self.lock:
            log.info("scanning for idle connections..")
            idle_fs = [fs_name for fs_name,conn in self.connections.iteritems()
                       if conn.is_connection_idle(ConnectionPool.CONNECTION_IDLE_INTERVAL)]
            for fs_name in idle_fs:
                log.info("cleaning up connection for '{}'".format(fs_name))
                self._del_fs_handle(fs_name)

    def get_fs_handle(self, fs_name):
        with self.lock:
            conn = None
            try:
                conn = self.connections.get(fs_name, None)
                if conn:
                    if conn.is_connection_valid():
                        return conn.get_fs_handle()
                    else:
                        # filesystem id changed beneath us (or the filesystem does not exist).
                        # this is possible if the filesystem got removed (and recreated with
                        # same name) via "ceph fs rm/new" mon command.
                        log.warning("filesystem id changed for volume '{0}', reconnecting...".format(fs_name))
                        self._del_fs_handle(fs_name)
                conn = ConnectionPool.Connection(self.mgr, fs_name)
                conn.connect()
            except cephfs.Error as e:
                # try to provide a better error string if possible
                if e.args[0] == errno.ENOENT:
                    raise VolumeException(
                        -errno.ENOENT, "Volume '{0}' not found".format(fs_name))
                raise VolumeException(-e.args[0], e.args[1])
            self.connections[fs_name] = conn
            return conn.get_fs_handle()

    def put_fs_handle(self, fs_name):
        with self.lock:
            conn = self.connections.get(fs_name, None)
            if conn:
                conn.put_fs_handle()

    def _del_fs_handle(self, fs_name):
        conn = self.connections.pop(fs_name, None)
        if conn:
            conn.del_fs_handle()
    def del_fs_handle(self, fs_name):
        with self.lock:
            self._del_fs_handle(fs_name)

class VolumeClient(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.connection_pool = ConnectionPool(self.mgr)
        # TODO: make thread pool size configurable
        self.purge_queue = ThreadPoolPurgeQueueMixin(self, 4)
        # on startup, queue purge job for available volumes to kickstart
        # purge for leftover subvolume entries in trash. note that, if the
        # trash directory does not exist or if there are no purge entries
        # available for a volume, the volume is removed from the purge
        # job list.
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            self.purge_queue.queue_purge_job(fs['mdsmap']['fs_name'])

    def cluster_log(self, msg, lvl=None):
        """
        log to cluster log with default log level as WARN.
        """
        if not lvl:
            lvl = self.mgr.CLUSTER_LOG_PRIO_WARN
        self.mgr.cluster_log("cluster", lvl, msg)

    def gen_pool_names(self, volname):
        """
        return metadata and data pool name (from a filesystem/volume name) as a tuple
        """
        return "cephfs.{}.meta".format(volname), "cephfs.{}.data".format(volname)

    def get_fs(self, fs_name):
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            if fs['mdsmap']['fs_name'] == fs_name:
                return fs
        return None

    def get_mds_names(self, fs_name):
        fs = self.get_fs(fs_name)
        if fs is None:
            return []
        return [mds['name'] for mds in fs['mdsmap']['info'].values()]

    def volume_exists(self, volname):
        return self.get_fs(volname) is not None

    def volume_exception_to_retval(self, ve):
        """
        return a tuple representation from a volume exception
        """
        return ve.to_tuple()

    def create_pool(self, pool_name):
        # create the given pool
        command = {'prefix': 'osd pool create', 'pool': pool_name}
        r, outb, outs = self.mgr.mon_command(command)
        if r != 0:
            return r, outb, outs

        return r, outb, outs

    def remove_pool(self, pool_name):
        command = {'prefix': 'osd pool rm', 'pool': pool_name, 'pool2': pool_name,
                   'yes_i_really_really_mean_it': True}
        return self.mgr.mon_command(command)

    def create_filesystem(self, fs_name, metadata_pool, data_pool):
        command = {'prefix': 'fs new', 'fs_name': fs_name, 'metadata': metadata_pool,
                   'data': data_pool}
        return self.mgr.mon_command(command)

    def remove_filesystem(self, fs_name, confirm):
        if confirm != "--yes-i-really-mean-it":
            return -errno.EPERM, "", "WARNING: this will *PERMANENTLY DESTROY* all data " \
                "stored in the filesystem '{0}'. If you are *ABSOLUTELY CERTAIN* " \
                "that is what you want, re-issue the command followed by " \
                "--yes-i-really-mean-it.".format(fs_name)

        command = {'prefix': 'fs fail', 'fs_name': fs_name}
        r, outb, outs = self.mgr.mon_command(command)
        if r != 0:
            return r, outb, outs

        command = {'prefix': 'fs rm', 'fs_name': fs_name, 'yes_i_really_mean_it': True}
        return self.mgr.mon_command(command)

    def create_mds(self, fs_name):
        spec = orchestrator.StatelessServiceSpec(fs_name)
        try:
            completion = self.mgr.add_mds(spec)
            self.mgr._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            return 0, "", "Volume created successfully (no MDS daemons created)"
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to create MDS daemons")
            return -errno.EINVAL, "", str(e)
        return 0, "", ""

    ### volume operations -- create, rm, ls

    def create_volume(self, volname):
        """
        create volume  (pool, filesystem and mds)
        """
        metadata_pool, data_pool = self.gen_pool_names(volname)
        # create pools
        r, outs, outb = self.create_pool(metadata_pool)
        if r != 0:
            return r, outb, outs
        r, outb, outs = self.create_pool(data_pool)
        if r != 0:
            return r, outb, outs
        # create filesystem
        r, outb, outs = self.create_filesystem(volname, metadata_pool, data_pool)
        if r != 0:
            log.error("Filesystem creation error: {0} {1} {2}".format(r, outb, outs))
            return r, outb, outs
        # create mds
        return self.create_mds(volname)

    def delete_volume(self, volname, confirm):
        """
        delete the given module (tear down mds, remove filesystem)
        """
        self.purge_queue.cancel_purge_job(volname)
        self.connection_pool.del_fs_handle(volname)
        # Tear down MDS daemons
        try:
            completion = self.mgr.remove_mds(volname)
            self.mgr._orchestrator_wait([completion])
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            log.warning("OrchestratorError, not tearing down MDS daemons")
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to tear down MDS daemons")
            return -errno.EINVAL, "", str(e)

        # In case orchestrator didn't tear down MDS daemons cleanly, or
        # there was no orchestrator, we force the daemons down.
        if self.volume_exists(volname):
            r, outb, outs = self.remove_filesystem(volname, confirm)
            if r != 0:
                return r, outb, outs
        else:
            err = "Filesystem not found for volume '{0}'".format(volname)
            log.warning(err)
            return -errno.ENOENT, "", err
        metadata_pool, data_pool = self.gen_pool_names(volname)
        r, outb, outs = self.remove_pool(metadata_pool)
        if r != 0:
            return r, outb, outs
        return self.remove_pool(data_pool)

    def list_volumes(self):
        result = []
        fs_map = self.mgr.get("fs_map")
        for f in fs_map['filesystems']:
            result.append({'name': f['mdsmap']['fs_name']})
        return 0, json.dumps(result, indent=2), ""

    def group_exists(self, sv, spec):
        # default group need not be explicitly created (as it gets created
        # at the time of subvolume, snapshot and other create operations).
        return spec.is_default_group() or sv.get_group_path(spec)

    @staticmethod
    def octal_str_to_decimal_int(mode):
        try:
            return int(mode, 8)
        except ValueError:
            raise VolumeException(-errno.EINVAL, "Invalid mode '{0}'".format(mode))

    def connection_pool_wrap(func):
        """
        decorator that wraps subvolume calls by fetching filesystem handle
        from the connection pool when fs_handle argument is empty, otherwise
        just invoke func with the passed in filesystem handle. Also handles
        call made to non-existent volumes (only when fs_handle is empty).
        """
        def conn_wrapper(self, fs_handle, **kwargs):
            fs_h = fs_handle
            fs_name = kwargs['vol_name']
            # note that force arg is available for remove type commands
            force = kwargs.get('force', False)

            # fetch the connection from the pool
            if not fs_handle:
                try:
                    fs_h = self.connection_pool.get_fs_handle(fs_name)
                except VolumeException as ve:
                    if not force:
                        return self.volume_exception_to_retval(ve)
                    return 0, "", ""

            # invoke the actual routine w/ fs handle
            result = func(self, fs_h, **kwargs)

            # hand over the connection back to the pool
            if fs_h:
                self.connection_pool.put_fs_handle(fs_name)
            return result
        return conn_wrapper

    def nametojson(self, names):
        """
        convert the list of names to json
        """

        namedict = []
        for i in range(len(names)):
            namedict.append({'name': names[i].decode('utf-8')})
        return json.dumps(namedict, indent=2)

    ### subvolume operations

    @connection_pool_wrap
    def create_subvolume(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        mode       = kwargs['mode']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, create it with " \
                        "`ceph fs subvolumegroup create` before creating subvolumes".format(groupname))
                sv.create_subvolume(spec, size, pool=pool, mode=self.octal_str_to_decimal_int(mode))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def remove_subvolume(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if self.group_exists(sv, spec):
                    sv.remove_subvolume(spec, force)
                    self.purge_queue.queue_purge_job(volname)
                elif not force:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, cannot remove " \
                        "subvolume '{1}'".format(groupname, subvolname))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def resize_subvolume(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        subvolname = kwargs['sub_name']
        newsize    = kwargs['new_size']
        groupname  = kwargs['group_name']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, create it with " \
                        "'ceph fs subvolumegroup create' before creating or resizing subvolumes".format(groupname))
                subvolpath = sv.get_subvolume_path(spec)
                if not subvolpath:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume '{0}' not found, create it with " \
                        "'ceph fs subvolume create' before resizing subvolumes".format(subvolname))

                try:
                    newsize = int(newsize)
                except ValueError:
                    newsize = newsize.lower()
                    nsize, usedbytes = sv.resize_infinite(subvolpath, newsize)
                    ret = 0, json.dumps([{'bytes_used': usedbytes}, {'bytes_quota': nsize}, {'bytes_pcent': "undefined"}], indent=2), ""
                else:
                    noshrink = kwargs['no_shrink']
                    nsize, usedbytes = sv.resize_subvolume(subvolpath, newsize, noshrink)
                    ret = 0, json.dumps([{'bytes_used': usedbytes}, {'bytes_quota': nsize},
                                         {'bytes_pcent': '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}], indent=2), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def subvolume_getpath(self, fs_handle, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found".format(groupname))
                path = sv.get_subvolume_path(spec)
                if not path:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume '{0}' not found".format(subvolname))
                ret = 0, path.decode("utf-8"), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def list_subvolumes(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        groupname  = kwargs['group_name']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(None, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found".format(groupname))
                path = sv.get_group_path(spec)
                # When default subvolume group is not yet created we just return an empty list.
                if path is None:
                    ret = 0, '[]', ""
                else:
                    subvolumes = sv.get_dir_entries(path)
                    ret = 0, self.nametojson(subvolumes), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### subvolume snapshot

    @connection_pool_wrap
    def create_subvolume_snapshot(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, cannot create " \
                        "snapshot '{1}'".format(groupname, snapname))
                if not sv.get_subvolume_path(spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume '{0}' not found, cannot create snapshot " \
                        "'{1}'".format(subvolname, snapname))
                sv.create_subvolume_snapshot(spec, snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def remove_subvolume_snapshot(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if self.group_exists(sv, spec):
                    if sv.get_subvolume_path(spec):
                        sv.remove_subvolume_snapshot(spec, snapname, force)
                    elif not force:
                        raise VolumeException(
                            -errno.ENOENT, "Subvolume '{0}' not found, cannot remove " \
                            "subvolume snapshot '{1}'".format(subvolname, snapname))
                elif not force:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' already removed, cannot " \
                        "remove subvolume snapshot '{1}'".format(groupname, snapname))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def list_subvolume_snapshots(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(subvolname, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found".format(groupname))

                if sv.get_subvolume_path(spec) == None:
                    raise VolumeException(-errno.ENOENT,
                                          "Subvolume '{0}' not found".format(subvolname))

                path = spec.make_subvol_snapdir_path(self.mgr.rados.conf_get('client_snapdir'))
                snapshots = sv.get_dir_entries(path)
                ret = 0, self.nametojson(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret


    ### group operations

    @connection_pool_wrap
    def create_subvolume_group(self, fs_handle, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        pool      = kwargs['pool_layout']
        mode      = kwargs['mode']

        try:
            # TODO: validate that subvol size fits in volume size
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec("", groupname)
                sv.create_group(spec, pool=pool, mode=self.octal_str_to_decimal_int(mode))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def remove_subvolume_group(self, fs_handle, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        force     = kwargs['force']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                # TODO: check whether there are no subvolumes in the group
                spec = SubvolumeSpec("", groupname)
                sv.remove_group(spec, force)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def getpath_subvolume_group(self, fs_handle, **kwargs):
        groupname  = kwargs['group_name']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec("", groupname)
                path = sv.get_group_path(spec)
                if path is None:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found".format(groupname))
                return 0, path.decode("utf-8"), ""
        except VolumeException as ve:
            return self.volume_exception_to_retval(ve)

    @connection_pool_wrap
    def list_subvolume_groups(self, fs_handle, **kwargs):
        ret = 0, "", ""

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                subvolumegroups = sv.get_dir_entries(SubvolumeSpec.DEFAULT_SUBVOL_PREFIX)
                ret = 0, self.nametojson(subvolumegroups), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### group snapshot

    @connection_pool_wrap
    def create_subvolume_group_snapshot(self, fs_handle, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec("", groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, cannot create " \
                        "snapshot '{1}'".format(groupname, snapname))
                sv.create_group_snapshot(spec, snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def remove_subvolume_group_snapshot(self, fs_handle, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']
        force     = kwargs['force']
        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec("", groupname)
                if self.group_exists(sv, spec):
                    sv.remove_group_snapshot(spec, snapname, force)
                elif not force:
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found, cannot " \
                        "remove it".format(groupname))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def list_subvolume_group_snapshots(self, fs_handle, **kwargs):
        ret        = 0, "", ""
        groupname  = kwargs['group_name']

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(None, groupname)
                if not self.group_exists(sv, spec):
                    raise VolumeException(
                        -errno.ENOENT, "Subvolume group '{0}' not found".format(groupname))

                path = spec.make_group_snapdir_path(self.mgr.rados.conf_get('client_snapdir'))
                snapshots = sv.get_dir_entries(path)
                ret = 0, self.nametojson(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def get_subvolume_trash_entry(self, fs_handle, **kwargs):
        ret = None
        volname = kwargs['vol_name']
        exclude = kwargs.get('exclude_entries', [])

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec("", "")
                path = sv.get_trash_entry(spec, exclude)
                ret = 0, path, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    @connection_pool_wrap
    def purge_subvolume_trash_entry(self, fs_handle, **kwargs):
        ret = 0, "", ""
        volname = kwargs['vol_name']
        purge_dir = kwargs['purge_dir']
        should_cancel = kwargs.get('should_cancel', lambda: False)

        try:
            with SubVolume(self.mgr, fs_handle) as sv:
                spec = SubvolumeSpec(purge_dir.decode('utf-8'), "")
                sv.purge_subvolume(spec, should_cancel)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

import time
import errno
import logging
import sys

try:
    from typing import List
except ImportError:
    pass  # For typing only

from contextlib import contextmanager
from threading import Lock, Condition

if sys.version_info >= (3, 3):
    from threading import Timer
else:
    from threading import _Timer as Timer

import cephfs
import orchestrator

from .lock import GlobalLock
from ..exception import VolumeException
from ..fs_util import create_pool, remove_pool, create_filesystem, \
    remove_filesystem, create_mds, volume_exists

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

        def put_fs_handle(self, notify):
            assert self.ops_in_progress > 0
            self.ops_in_progress -= 1
            if self.ops_in_progress == 0:
                notify()

        def del_fs_handle(self, waiter):
            if waiter:
                while self.ops_in_progress != 0:
                    waiter()
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
            log.debug("self.fs_id={0}, fs_id={1}".format(self.fs_id, fs_id))
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
            try:
                assert self.fs
                assert self.ops_in_progress == 0
                log.info("disconnecting from cephfs '{0}'".format(self.fs_name))
                self.fs.shutdown()
                self.fs = None
            except Exception as e:
                log.debug("disconnect: ({0})".format(e))
                raise

        def abort(self):
            assert self.fs
            assert self.ops_in_progress == 0
            log.info("aborting connection from cephfs '{0}'".format(self.fs_name))
            self.fs.abort_conn()
            log.info("abort done from cephfs '{0}'".format(self.fs_name))
            self.fs = None

    class RTimer(Timer):
        """
        recurring timer variant of Timer
        """
        def run(self):
            try:
                while not self.finished.is_set():
                    self.finished.wait(self.interval)
                    self.function(*self.args, **self.kwargs)
                self.finished.set()
            except Exception as e:
                log.error("ConnectionPool.RTimer: %s", e)
                raise

    # TODO: make this configurable
    TIMER_TASK_RUN_INTERVAL = 30.0  # seconds
    CONNECTION_IDLE_INTERVAL = 60.0 # seconds

    def __init__(self, mgr):
        self.mgr = mgr
        self.connections = {}
        self.lock = Lock()
        self.cond = Condition(self.lock)
        self.timer_task = ConnectionPool.RTimer(ConnectionPool.TIMER_TASK_RUN_INTERVAL,
                                                self.cleanup_connections)
        self.timer_task.start()

    def cleanup_connections(self):
        with self.lock:
            log.info("scanning for idle connections..")
            idle_fs = [fs_name for fs_name,conn in self.connections.items()
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
                conn.put_fs_handle(notify=lambda: self.cond.notifyAll())

    def _del_fs_handle(self, fs_name, wait=False):
        conn = self.connections.pop(fs_name, None)
        if conn:
            conn.del_fs_handle(waiter=None if not wait else lambda: self.cond.wait())

    def del_fs_handle(self, fs_name, wait=False):
        with self.lock:
            self._del_fs_handle(fs_name, wait)

    def del_all_handles(self):
        with self.lock:
            for fs_name in list(self.connections.keys()):
                log.info("waiting for pending ops for '{}'".format(fs_name))
                self._del_fs_handle(fs_name, wait=True)
                log.info("pending ops completed for '{}'".format(fs_name))
            # no new connections should have been initialized since its
            # guarded on shutdown.
            assert len(self.connections) == 0

def gen_pool_names(volname):
    """
    return metadata and data pool name (from a filesystem/volume name) as a tuple
    """
    return "cephfs.{}.meta".format(volname), "cephfs.{}.data".format(volname)

def get_pool_names(mgr, volname):
    """
    return metadata and data pools (list) names of volume as a tuple
    """
    fs_map = mgr.get("fs_map")
    metadata_pool_id = None
    data_pool_ids = [] # type: List[int]
    for f in fs_map['filesystems']:
        if volname == f['mdsmap']['fs_name']:
            metadata_pool_id = f['mdsmap']['metadata_pool']
            data_pool_ids = f['mdsmap']['data_pools']
            break
    if metadata_pool_id is None:
        return None, None

    osdmap = mgr.get("osd_map")
    pools = dict([(p['pool'], p['pool_name']) for p in osdmap['pools']])
    metadata_pool = pools[metadata_pool_id]
    data_pools = [pools[id] for id in data_pool_ids]
    return metadata_pool, data_pools

def create_volume(mgr, volname):
    """
    create volume  (pool, filesystem and mds)
    """
    metadata_pool, data_pool = gen_pool_names(volname)
    # create pools
    r, outs, outb = create_pool(mgr, metadata_pool, 16)
    if r != 0:
        return r, outb, outs
    r, outb, outs = create_pool(mgr, data_pool, 8)
    if r != 0:
        #cleanup
        remove_pool(mgr, metadata_pool)
        return r, outb, outs
    # create filesystem
    r, outb, outs = create_filesystem(mgr, volname, metadata_pool, data_pool)
    if r != 0:
        log.error("Filesystem creation error: {0} {1} {2}".format(r, outb, outs))
        #cleanup
        remove_pool(mgr, data_pool)
        remove_pool(mgr, metadata_pool)
        return r, outb, outs
    # create mds
    return create_mds(mgr, volname)

def delete_volume(mgr, volname, metadata_pool, data_pools):
    """
    delete the given module (tear down mds, remove filesystem, remove pools)
    """
    # Tear down MDS daemons
    try:
        completion = mgr.remove_stateless_service("mds", volname)
        mgr._orchestrator_wait([completion])
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
    if volume_exists(mgr, volname):
        r, outb, outs = remove_filesystem(mgr, volname)
        if r != 0:
            return r, outb, outs
    else:
        err = "Filesystem not found for volume '{0}'".format(volname)
        log.warning(err)
        return -errno.ENOENT, "", err
    r, outb, outs = remove_pool(mgr, metadata_pool)
    if r != 0:
        return r, outb, outs

    for data_pool in data_pools:
        r, outb, outs = remove_pool(mgr, data_pool)
        if r != 0:
            return r, outb, outs
    result_str = "metadata pool: {0} data pool: {1} removed".format(metadata_pool, str(data_pools))
    return r, result_str, ""

def list_volumes(mgr):
    """
    list all filesystem volumes.

    :param: None
    :return: None
    """
    result = []
    fs_map = mgr.get("fs_map")
    for f in fs_map['filesystems']:
        result.append({'name': f['mdsmap']['fs_name']})
    return result

@contextmanager
def open_volume(vc, volname):
    """
    open a volume for exclusive access. This API is to be used as a context manager.

    :param vc: volume client instance
    :param volname: volume name
    :return: yields a volume handle (ceph filesystem handle)
    """
    if vc.is_stopping():
        raise VolumeException(-errno.ESHUTDOWN, "shutdown in progress")

    g_lock = GlobalLock()
    fs_handle = vc.connection_pool.get_fs_handle(volname)
    try:
        with g_lock.lock_op():
            yield fs_handle
    finally:
        vc.connection_pool.put_fs_handle(volname)

@contextmanager
def open_volume_lockless(vc, volname):
    """
    open a volume with shared access. This API is to be used as a context manager.

    :param vc: volume client instance
    :param volname: volume name
    :return: yields a volume handle (ceph filesystem handle)
    """
    if vc.is_stopping():
        raise VolumeException(-errno.ESHUTDOWN, "shutdown in progress")

    fs_handle = vc.connection_pool.get_fs_handle(volname)
    try:
        yield fs_handle
    finally:
        vc.connection_pool.put_fs_handle(volname)

import errno
import logging
import sys

from contextlib import contextmanager

import orchestrator

from .lock import GlobalLock
from ..exception import VolumeException
from ..fs_util import create_pool, remove_pool, create_filesystem, \
    remove_filesystem, create_mds, volume_exists
from mgr_util import open_filesystem

log = logging.getLogger(__name__)


def gen_pool_names(volname):
    """
    return metadata and data pool name (from a filesystem/volume name) as a tuple
    """
    return "cephfs.{}.meta".format(volname), "cephfs.{}.data".format(volname)

def create_volume(mgr, volname, placement):
    """
    create volume  (pool, filesystem and mds)
    """
    metadata_pool, data_pool = gen_pool_names(volname)
    # create pools
    r, outs, outb = create_pool(mgr, metadata_pool)
    if r != 0:
        return r, outb, outs
    r, outb, outs = create_pool(mgr, data_pool)
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
    return create_mds(mgr, volname, placement)


def delete_volume(mgr, volname):
    """
    delete the given module (tear down mds, remove filesystem)
    """
    # Tear down MDS daemons
    try:
        completion = mgr.remove_service('mds.' + volname)
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
    metadata_pool, data_pool = gen_pool_names(volname)
    r, outb, outs = remove_pool(mgr, metadata_pool)
    if r != 0:
        return r, outb, outs
    return remove_pool(mgr, data_pool)


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
    open a volume for exclusive access. This API is to be used as a contextr
    manager.

    :param vc: volume client instance
    :param volname: volume name
    :return: yields a volume handle (ceph filesystem handle)
    """
    g_lock = GlobalLock()
    with g_lock.lock_op():
        with open_filesystem(vc, volname) as fs_handle:
            yield fs_handle


@contextmanager
def open_volume_lockless(vc, volname):
    """
    open a volume with shared access. This API is to be used as a context
    manager.

    :param vc: volume client instance
    :param volname: volume name
    :return: yields a volume handle (ceph filesystem handle)
    """
    with open_filesystem(vc, volname) as fs_handle:
        yield fs_handle

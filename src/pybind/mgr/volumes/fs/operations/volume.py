import errno
import logging
import sys

from typing import List

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


def delete_volume(mgr, volname, metadata_pool, data_pools):
    """
    delete the given module (tear down mds, remove filesystem, remove pools)
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

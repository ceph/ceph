import errno
import logging
import os

from typing import List, Tuple

from contextlib import contextmanager

import orchestrator

from .lock import GlobalLock
from ..exception import VolumeException, IndexException
from ..fs_util import create_pool, remove_pool, rename_pool, create_filesystem, \
    remove_filesystem, rename_filesystem, create_mds, volume_exists, listdir
from .trash import Trash
from mgr_util import open_filesystem, CephfsConnectionException
from .clone_index import open_clone_index

log = logging.getLogger(__name__)

def gen_pool_names(volname):
    """
    return metadata and data pool name (from a filesystem/volume name) as a tuple
    """
    return "cephfs.{}.meta".format(volname), "cephfs.{}.data".format(volname)

def get_mds_map(mgr, volname):
    """
    return mdsmap for a volname
    """
    mds_map = None
    fs_map = mgr.get("fs_map")
    for f in fs_map['filesystems']:
        if volname == f['mdsmap']['fs_name']:
            return f['mdsmap']
    return mds_map

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

def get_pool_ids(mgr, volname):
    """
    return metadata and data pools (list) id of volume as a tuple
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
    return metadata_pool_id, data_pool_ids

def create_volume(mgr, volname, placement):
    """
    create volume  (pool, filesystem and mds)
    """
    metadata_pool, data_pool = gen_pool_names(volname)
    # create pools
    r, outb, outs = create_pool(mgr, metadata_pool)
    if r != 0:
        return r, outb, outs
    # default to a bulk pool for data. In case autoscaling has been disabled
    # for the cluster with `ceph osd pool set noautoscale`, this will have no effect.
    r, outb, outs = create_pool(mgr, data_pool, bulk=True)
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
    return create_mds(mgr, volname, placement)


def delete_volume(mgr, volname, metadata_pool, data_pools):
    """
    delete the given module (tear down mds, remove filesystem, remove pools)
    """
    # Tear down MDS daemons
    try:
        completion = mgr.remove_service('mds.' + volname)
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

def rename_volume(mgr, volname: str, newvolname: str) -> Tuple[int, str, str]:
    """
    rename volume (orch MDS service, file system, pools)
    """
    # To allow volume rename to be idempotent, check whether orch managed MDS
    # service is already renamed. If so, skip renaming MDS service.
    completion = None
    rename_mds_service = True
    try:
        completion = mgr.describe_service(
            service_type='mds', service_name=f"mds.{newvolname}", refresh=True)
        orchestrator.raise_if_exception(completion)
    except (ImportError, orchestrator.OrchestratorError):
        log.warning("Failed to fetch orch service mds.%s", newvolname)
    except Exception as e:
        # Don't let detailed orchestrator exceptions (python backtraces)
        # bubble out to the user
        log.exception("Failed to fetch orch service mds.%s", newvolname)
        return -errno.EINVAL, "", str(e)
    if completion and completion.result:
        rename_mds_service = False

    # Launch new MDS service matching newvolname
    completion = None
    remove_mds_service = False
    if rename_mds_service:
        try:
            completion = mgr.describe_service(
                service_type='mds', service_name=f"mds.{volname}", refresh=True)
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            log.warning("Failed to fetch orch service mds.%s", volname)
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to fetch orch service mds.%s", volname)
            return -errno.EINVAL, "", str(e)
        if completion and completion.result:
            svc = completion.result[0]
            placement = svc.spec.placement.pretty_str()
            create_mds(mgr, newvolname, placement)
            remove_mds_service = True

    # rename_filesytem is idempotent
    r, outb, outs = rename_filesystem(mgr, volname, newvolname)
    if r != 0:
        errmsg = f"Failed to rename file system '{volname}' to '{newvolname}'"
        log.error("Failed to rename file system '%s' to '%s'", volname, newvolname)
        outs = f'{errmsg}; {outs}'
        return r, outb, outs

    # Rename file system's metadata and data pools
    metadata_pool, data_pools = get_pool_names(mgr, newvolname)

    new_metadata_pool, new_data_pool = gen_pool_names(newvolname)
    if metadata_pool != new_metadata_pool:
        r, outb, outs =  rename_pool(mgr, metadata_pool, new_metadata_pool)
        if r != 0:
            errmsg = f"Failed to rename metadata pool '{metadata_pool}' to '{new_metadata_pool}'"
            log.error("Failed to rename metadata pool '%s' to '%s'", metadata_pool, new_metadata_pool)
            outs = f'{errmsg}; {outs}'
            return r, outb, outs

    data_pool_rename_failed = False
    # If file system has more than one data pool, then skip renaming
    # the data pools, and proceed to remove the old MDS service.
    if len(data_pools) > 1:
        data_pool_rename_failed = True
    else:
        data_pool = data_pools[0]
        if data_pool != new_data_pool:
            r, outb, outs = rename_pool(mgr, data_pool, new_data_pool)
            if r != 0:
                errmsg = f"Failed to rename data pool '{data_pool}' to '{new_data_pool}'"
                log.error("Failed to rename data pool '%s' to '%s'", data_pool, new_data_pool)
                outs = f'{errmsg}; {outs}'
                return r, outb, outs

    # Tear down old MDS service
    if remove_mds_service:
        try:
            completion = mgr.remove_service('mds.' + volname)
            orchestrator.raise_if_exception(completion)
        except (ImportError, orchestrator.OrchestratorError):
            log.warning("Failed to tear down orch service mds.%s", volname)
        except Exception as e:
            # Don't let detailed orchestrator exceptions (python backtraces)
            # bubble out to the user
            log.exception("Failed to tear down orch service mds.%s", volname)
            return -errno.EINVAL, "", str(e)

    outb = f"FS volume '{volname}' renamed to '{newvolname}'"
    if data_pool_rename_failed:
        outb += ". But failed to rename data pools as more than one data pool was found."

    return r, outb, ""

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


def get_pending_subvol_deletions_count(fs, path):
    """
    Get the number of pending subvolumes deletions.
    """
    trashdir = os.path.join(path, Trash.GROUP_NAME)
    try:
        num_pending_subvol_del = len(listdir(fs, trashdir, filter_entries=None, filter_files=False))
    except VolumeException as ve:
        if ve.errno == -errno.ENOENT:
            num_pending_subvol_del = 0

    return {'pending_subvolume_deletions': num_pending_subvol_del}


def get_all_pending_clones_count(self, mgr, vol_spec):
    pending_clones_cnt = 0
    index_path = ""
    fs_map = mgr.get('fs_map')
    for fs in fs_map['filesystems']:
        volname = fs['mdsmap']['fs_name']
        try:
            with open_volume(self, volname) as fs_handle:
                with open_clone_index(fs_handle, vol_spec) as index:
                    index_path = index.path.decode('utf-8')
                    pending_clones_cnt = pending_clones_cnt \
                                            + len(listdir(fs_handle, index_path,
                                                          filter_entries=None, filter_files=False))
        except IndexException as e:
            if e.errno == -errno.ENOENT:
                continue
            raise VolumeException(-e.args[0], e.args[1])
        except VolumeException as ve:
            log.error("error fetching clone entry for volume '{0}' ({1})".format(volname, ve))
            raise ve

    return pending_clones_cnt


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
        try:
            with open_filesystem(vc, volname) as fs_handle:
                yield fs_handle
        except CephfsConnectionException as ce:
            raise VolumeException(ce.errno, ce.error_str)


@contextmanager
def open_volume_lockless(vc, volname):
    """
    open a volume with shared access. This API is to be used as a context
    manager.

    :param vc: volume client instance
    :param volname: volume name
    :return: yields a volume handle (ceph filesystem handle)
    """
    try:
        with open_filesystem(vc, volname) as fs_handle:
            yield fs_handle
    except CephfsConnectionException as ce:
        raise VolumeException(ce.errno, ce.error_str)
